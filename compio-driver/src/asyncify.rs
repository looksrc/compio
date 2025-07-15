use std::{
    fmt,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use crossbeam_channel::{Receiver, Sender, TrySendError, bounded};

/// An error that may be emitted when all worker threads are busy. It simply
/// returns the dispatchable value with a convenient [`fmt::Debug`] and
/// [`fmt::Display`] implementation.
///
/// 当分发阻塞任务的时候，有可能没有空闲线程。此时就会返回这个错误，
/// 并携带被分发的内容。
#[derive(Copy, Clone, PartialEq, Eq)]
pub struct DispatchError<T>(pub T);

impl<T> DispatchError<T> {
    /// Consume the error, yielding the dispatchable that failed to be sent.
    pub fn into_inner(self) -> T {
        self.0
    }
}

impl<T> fmt::Debug for DispatchError<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        "DispatchError(..)".fmt(f)
    }
}

impl<T> fmt::Display for DispatchError<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        "all threads are busy".fmt(f)
    }
}

impl<T> std::error::Error for DispatchError<T> {}

/// 盒装的Dispatchable特质对象，比如Concrete值。
type BoxedDispatchable = Box<dyn Dispatchable + Send>;

/// A trait for dispatching a closure. It's implemented for all `FnOnce() + Send
/// + 'static` but may also be implemented for any other types that are `Send`
///   and `'static`.
///
/// 可分发接口，用于分发(即执行)阻塞操作，实现接口这才可以被分发：
/// - 被动实现者：FnOnce() + Send + 'static
/// - 主动实现者：必须符合 Send + 'static
pub trait Dispatchable: Send + 'static {
    /// Run the dispatchable
    fn run(self: Box<Self>);
}

impl<F> Dispatchable for F
where
    F: FnOnce() + Send + 'static,
{
    fn run(self: Box<Self>) {
        (*self)()
    }
}

struct CounterGuard(Arc<AtomicUsize>);

impl Drop for CounterGuard {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::AcqRel);
    }
}

/// 构造一个闭包，在阻塞任务处理线程中启动并常驻。
fn worker(
    receiver: Receiver<BoxedDispatchable>,
    counter: Arc<AtomicUsize>,
    timeout: Duration,
) -> impl FnOnce() {
    move || {
        counter.fetch_add(1, Ordering::AcqRel);
        let _guard = CounterGuard(counter);
        while let Ok(f) = receiver.recv_timeout(timeout) {
            f.run();
        }
    }
}

/// A thread pool to perform blocking operations in other threads.
///
/// 阻塞线程池，用于执行阻塞操作。
/// - sender：任务发送端。分发者持有发送端，向池中发送任务。
/// - receiver：任务接收端。池中线程持有接收端，进行任务接收。
/// - counter：当前线程数。
/// - thread_limit:线程数上限。
#[derive(Debug, Clone)]
pub struct AsyncifyPool {
    sender: Sender<BoxedDispatchable>,
    receiver: Receiver<BoxedDispatchable>,
    counter: Arc<AtomicUsize>,
    thread_limit: usize,
    recv_timeout: Duration,
}

impl AsyncifyPool {
    /// Create [`AsyncifyPool`] with thread number limit and channel receive
    /// timeout.
    pub fn new(thread_limit: usize, recv_timeout: Duration) -> Self {
        let (sender, receiver) = bounded(0);
        Self {
            sender,
            receiver,
            counter: Arc::new(AtomicUsize::new(0)),
            thread_limit,
            recv_timeout,
        }
    }

    /// Send a dispatchable, usually a closure, to another thread. Usually the
    /// user should not use it. When all threads are busy and thread number
    /// limit has been reached, it will return an error with the original
    /// dispatchable.
    ///
    /// 分发阻塞操作(在本compio-dispatcher项目中是Concrete类型)。
    /// - 通过通道发送端，向线程集群发送阻塞任务。
    /// - 如果，发送成功，表明任务分发成功了。
    /// - 如果，因为通道满了而发送失败，且线程数未超上限，
    ///   则启动一个新线程并再次发送任务。
    /// - 如果，因为没有任何接收端而发送失败，则表明所有工作线程都不在了。
    ///   (实际上不会出现这种情况)
    pub fn dispatch<D: Dispatchable>(&self, f: D) -> Result<(), DispatchError<D>> {
        match self.sender.try_send(Box::new(f) as BoxedDispatchable) {
            Ok(_) => Ok(()),
            Err(e) => match e {
                TrySendError::Full(f) => {
                    if self.counter.load(Ordering::Acquire) >= self.thread_limit {
                        // Safety: we can ensure the type
                        Err(DispatchError(*unsafe {
                            Box::from_raw(Box::into_raw(f).cast())
                        }))
                    } else {
                        std::thread::spawn(worker(
                            self.receiver.clone(),
                            self.counter.clone(),
                            self.recv_timeout,
                        ));
                        self.sender.send(f).expect("the channel should not be full");
                        Ok(())
                    }
                }
                TrySendError::Disconnected(_) => {
                    unreachable!("receiver should not all disconnected")
                }
            },
        }
    }
}
