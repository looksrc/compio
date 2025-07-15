//! Multithreading dispatcher for compio.
//!
//! 多线程分发器：
//! - 用于将异步操作或同步操作分发给对应的后端线程池执行。
//! - 构建两个线程池，分别用于执行异步操作(异步池)、阻塞操作(阻塞池)。
//!
//! 两个线程池：
//! - 异步池：每个线程启动一个异步运行时，不断的接收和执行异步操作。
//!   - 前台执行
//!   - 后台执行
//! - 阻塞池：由AsyncifyPool负责。

#![warn(missing_docs)]

use std::{
    future::Future,
    io,
    num::NonZeroUsize,
    panic::resume_unwind,
    thread::{JoinHandle, available_parallelism},
};

use compio_driver::{AsyncifyPool, DispatchError, Dispatchable, ProactorBuilder};
use compio_runtime::{JoinHandle as CompioJoinHandle, Runtime};
use flume::{Sender, unbounded};
use futures_channel::oneshot;

type Spawning = Box<dyn Spawnable + Send>;

/// 可孵化接口
/// - 提供spawn：将自身`Self`孵化到目标句柄`handle`中执行，
///   执行完成后会通过单发通道回调。
/// - 返回值：任务异步等待者句柄。
trait Spawnable {
    fn spawn(self: Box<Self>, handle: &Runtime) -> CompioJoinHandle<()>;
}

/// Concrete type for the closure we're sending to worker threads
///
/// 操作块
/// - callback：通道发送端，用于本操作执行完成后，进行回调；
/// - func：操作块索要执行的动作。
struct Concrete<F, R> {
    callback: oneshot::Sender<R>,
    func: F,
}

impl<F, R> Concrete<F, R> {
    pub fn new(func: F) -> (Self, oneshot::Receiver<R>) {
        let (tx, rx) = oneshot::channel();
        (Self { callback: tx, func }, rx)
    }
}

impl<F, Fut, R> Spawnable for Concrete<F, R>
where
    F: FnOnce() -> Fut + Send + 'static,
    Fut: Future<Output = R>,
    R: Send + 'static,
{
    /// 当Concrete是一个异步块时，为其实现孵化接口
    /// - 孵化一个异步操作，操作块函数必须是一个异步函数。
    fn spawn(self: Box<Self>, handle: &Runtime) -> CompioJoinHandle<()> {
        let Concrete { callback, func } = *self;
        handle.spawn(async move {
            let res = func().await;
            callback.send(res).ok();
        })
    }
}

impl<F, R> Dispatchable for Concrete<F, R>
where
    F: FnOnce() -> R + Send + 'static,
    R: Send + 'static,
{
    /// 当Concrete是一个函数时，为其实现执行(分发)接口。
    fn run(self: Box<Self>) {
        let Concrete { callback, func } = *self;
        let res = func();
        callback.send(res).ok();
    }
}

/// The dispatcher. It manages the threads and dispatches the tasks.
///
/// 分发器
/// - 分发一个同步或异步操作，使之在对应的后端执行。
/// - 异步操作：后端是一个线程池，每个线程都是守护线程，负责接收和执行异步操作。
/// - 阻塞操作：后端是AsyncifyPool，由这个组件负责调度和执行阻塞操作。
#[derive(Debug)]
pub struct Dispatcher {
    /// 通道发送端，发送一个可孵化对象。
    sender: Sender<Spawning>,
    /// 线程等待者列表。
    threads: Vec<JoinHandle<()>>,
    /// 阻塞操作池，用于将操作发送到别的线程执行，实现异步化。
    pool: AsyncifyPool,
}

impl Dispatcher {
    /// Create the dispatcher with specified number of threads.
    ///
    /// 创建一分发器，指定线程数。
    pub(crate) fn new_impl(mut builder: DispatcherBuilder) -> io::Result<Self> {
        // 获取proactor构建器。
        let mut proactor_builder = builder.proactor_builder;
        // 强制本构建器创建的所有proactor之间可复用线程池。
        proactor_builder.force_reuse_thread_pool();
        // 获取proactor中的线程池，如果没有则新建一个。
        let pool = proactor_builder.create_or_get_thread_pool();
        // 创建一个无界队列，用于发送可孵化对象。
        let (sender, receiver) = unbounded::<Spawning>();

        // 初始化指定数量的线程。
        let threads = (0..builder.nthreads)
            .map({
                |index| {
                    let proactor_builder = proactor_builder.clone();
                    let receiver = receiver.clone();

                    // 线程属性配置：栈大小、线程名。
                    let thread_builder = std::thread::Builder::new();
                    let thread_builder = if let Some(s) = builder.stack_size {
                        thread_builder.stack_size(s)
                    } else {
                        thread_builder
                    };
                    let thread_builder = if let Some(f) = &mut builder.names {
                        thread_builder.name(f(index))
                    } else {
                        thread_builder
                    };

                    // 孵化执行异步任务的守护线程
                    // - 1.每个线程中启动一个异步运行时
                    // - 2.异步运行时不断地从队列接收异步操作，并将其孵化为异步任务。
                    // - 3.依据分发器的并发配置，决定是否需要等待孵化的任务完成后再孵化下一个任务。
                    thread_builder.spawn(move || {
                        Runtime::builder()
                            .with_proactor(proactor_builder)
                            .build()
                            .expect("cannot create compio runtime")
                            .block_on(async move {
                                while let Ok(f) = receiver.recv_async().await {
                                    let task = Runtime::with_current(|rt| f.spawn(rt));
                                    if builder.concurrent {
                                        task.detach()
                                    } else {
                                        task.await.ok();
                                    }
                                }
                            });
                    })
                }
            })
            .collect::<io::Result<Vec<_>>>()?;
        Ok(Self {
            sender,
            threads,
            pool,
        })
    }

    /// Create the dispatcher with default config.
    ///
    /// 创建一个默认配置的分发器。
    pub fn new() -> io::Result<Self> {
        Self::builder().build()
    }

    /// Create a builder to build a dispatcher.
    ///
    /// 创建分发器的构建器。
    pub fn builder() -> DispatcherBuilder {
        DispatcherBuilder::default()
    }

    /// Dispatch a task to the threads
    ///
    /// 分发异步操作。
    /// - 创建一个Concrete对象，以及一个回调接收端。
    /// - 将Concrete从通道发送给异步任务线程池执行，发送成功后返回回调接收端。
    /// - 发送失败后，返回错误，错误中携带被分发的原始异步函数。
    ///
    /// The provided `f` should be [`Send`] because it will be send to another
    /// thread before calling. The returned [`Future`] need not to be [`Send`]
    /// because it will be executed on only one thread.
    ///
    /// 被分发的函数必须实现[`Send`]，
    /// 函数执行获得的[`Future`]只会在当前线程执行，不需要[`Send`]。
    ///
    /// # Error
    ///
    /// If all threads have panicked, this method will return an error with the
    /// sent closure.
    ///
    /// # 错误
    ///
    /// 如果所有线程都恐慌了，此方法将返回一个错误，错误中回传发送的闭包。
    pub fn dispatch<Fn, Fut, R>(&self, f: Fn) -> Result<oneshot::Receiver<R>, DispatchError<Fn>>
    where
        Fn: (FnOnce() -> Fut) + Send + 'static,
        Fut: Future<Output = R> + 'static,
        R: Send + 'static,
    {
        let (concrete, rx) = Concrete::new(f);

        match self.sender.send(Box::new(concrete)) {
            Ok(_) => Ok(rx),
            Err(err) => {
                // SAFETY: We know the dispatchable we sent has type `Concrete<Fn, R>`
                let recovered =
                    unsafe { Box::from_raw(Box::into_raw(err.0) as *mut Concrete<Fn, R>) };
                Err(DispatchError(recovered.func))
            }
        }
    }

    /// Dispatch a blocking task to the threads.
    ///
    /// 分发阻塞操作。
    /// - 创建一个Concrete对象，以及一个回调接收端。
    /// - 将Concrete过继给阻塞线程池进行调度执行。
    ///
    /// Blocking pool of the dispatcher will be obtained from the proactor
    /// builder. So any configuration of the proactor's blocking pool will be
    /// applied to the dispatcher.
    ///
    /// # Error
    ///
    /// If all threads are busy and the thread pool is full, this method will
    /// return an error with the original closure. The limit can be configured
    /// with [`DispatcherBuilder::proactor_builder`] and
    /// [`ProactorBuilder::thread_pool_limit`].
    pub fn dispatch_blocking<Fn, R>(&self, f: Fn) -> Result<oneshot::Receiver<R>, DispatchError<Fn>>
    where
        Fn: FnOnce() -> R + Send + 'static,
        R: Send + 'static,
    {
        let (concrete, rx) = Concrete::new(f);

        self.pool
            .dispatch(concrete)
            .map_err(|e| DispatchError(e.0.func))?;

        Ok(rx)
    }

    /// Stop the dispatcher and wait for the threads to complete. If there is a
    /// thread panicked, this method will resume the panic.
    ///
    /// 停止分发器
    /// - 停止异步后端：关闭异步操作发送端，
    ///   新起一个阻塞操作专门负责合并异步池的所有线程。
    /// - 停止阻塞后端：不需要。
    /// - 如果捕获了线程恐慌，则在这里恢复恐慌。
    pub async fn join(self) -> io::Result<()> {
        drop(self.sender);
        let (tx, rx) = oneshot::channel::<Vec<_>>();
        if let Err(f) = self.pool.dispatch({
            move || {
                let results = self
                    .threads
                    .into_iter()
                    .map(|thread| thread.join())
                    .collect();
                tx.send(results).ok();
            }
        }) {
            std::thread::spawn(f.0);
        }
        let results = rx
            .await
            .map_err(|_| io::Error::other("the join task cancelled unexpectedly"))?;
        for res in results {
            res.unwrap_or_else(|e| resume_unwind(e));
        }
        Ok(())
    }
}

/// A builder for [`Dispatcher`].
///
/// 分发器的构建器。
/// - nthreads 异步池的线程数。
/// - concurrent 孵化的异步任务是否前台执行。
/// - stack_size 异步池线程栈大小。
/// - names 异步池线程起名。
/// - proactor_builder 驱动器构建器。
pub struct DispatcherBuilder {
    nthreads: usize,
    concurrent: bool,
    stack_size: Option<usize>,
    names: Option<Box<dyn FnMut(usize) -> String>>,
    proactor_builder: ProactorBuilder,
}

impl DispatcherBuilder {
    /// Create a builder with default settings.
    pub fn new() -> Self {
        Self {
            nthreads: available_parallelism().map(|n| n.get()).unwrap_or(1),
            concurrent: true,
            stack_size: None,
            names: None,
            proactor_builder: ProactorBuilder::new(),
        }
    }

    /// If execute tasks concurrently. Default to be `true`.
    ///
    /// When set to `false`, tasks are executed sequentially without any
    /// concurrency within the thread.
    pub fn concurrent(mut self, concurrent: bool) -> Self {
        self.concurrent = concurrent;
        self
    }

    /// Set the number of worker threads of the dispatcher. The default value is
    /// the CPU number. If the CPU number could not be retrieved, the
    /// default value is 1.
    pub fn worker_threads(mut self, nthreads: NonZeroUsize) -> Self {
        self.nthreads = nthreads.get();
        self
    }

    /// Set the size of stack of the worker threads.
    pub fn stack_size(mut self, s: usize) -> Self {
        self.stack_size = Some(s);
        self
    }

    /// Provide a function to assign names to the worker threads.
    pub fn thread_names(mut self, f: impl (FnMut(usize) -> String) + 'static) -> Self {
        self.names = Some(Box::new(f) as _);
        self
    }

    /// Set the proactor builder for the inner runtimes.
    pub fn proactor_builder(mut self, builder: ProactorBuilder) -> Self {
        self.proactor_builder = builder;
        self
    }

    /// Build the [`Dispatcher`].
    pub fn build(self) -> io::Result<Dispatcher> {
        Dispatcher::new_impl(self)
    }
}

impl Default for DispatcherBuilder {
    fn default() -> Self {
        Self::new()
    }
}
