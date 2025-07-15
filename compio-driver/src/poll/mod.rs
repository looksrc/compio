#[cfg_attr(all(doc, docsrs), doc(cfg(all())))]
#[allow(unused_imports)]
pub use std::os::fd::{AsFd, AsRawFd, BorrowedFd, OwnedFd, RawFd};
#[cfg(aio)]
use std::ptr::NonNull;
use std::{
    collections::{HashMap, VecDeque},
    io,
    num::NonZeroUsize,
    pin::Pin,
    sync::Arc,
    task::Poll,
    time::Duration,
};

use compio_log::{instrument, trace};
use crossbeam_queue::SegQueue;
pub(crate) use libc::{sockaddr_storage, socklen_t};
use polling::{Event, Events, Poller};

use crate::{AsyncifyPool, BufferPool, Entry, Key, ProactorBuilder, op::Interest, syscall};

pub(crate) mod op;

/// Abstraction of operations.
///
/// Poll操作码接口。在Poll驱动中执行的操作码必须实现此接口。
pub trait OpCode {
    /// Perform the operation before submit, and return [`Decision`] to
    /// indicate whether submitting the operation to polling is required.
    ///
    /// 决策操作的执行方式[`Decision`]：就地执行、异步执行、阻塞线程执行。
    fn pre_submit(self: Pin<&mut Self>) -> io::Result<Decision>;

    /// Get the operation type when an event is occurred.
    ///
    /// 当事件发生时，获取操作类型。
    fn op_type(self: Pin<&mut Self>) -> Option<OpType> {
        None
    }

    /// Perform the operation after received corresponding
    /// event. If this operation is blocking, the return value should be
    /// [`Poll::Ready`].
    ///
    /// 收到就绪事件后，真正的开始执行操作，一般是对操作系统函数的封装，
    /// 比如pread。
    ///
    /// 如果操作是阻塞的比如文件read()，最终都会返回[`Poll::Ready`]，
    /// 不存在未就绪的情况。 而网络描述符会设置为noblocking，
    /// 未读就绪时read()会直接报WouldBlock，返回[`Poll::Pending`]，不会阻塞。
    fn operate(self: Pin<&mut Self>) -> Poll<io::Result<usize>>;
}

/// Result of [`OpCode::pre_submit`].
///
/// 提交决策，是[`OpCode::pre_submit`]的执行结果，指示是否需要将操作提交轮询。
#[non_exhaustive]
pub enum Decision {
    /// Instant operation, no need to submit
    ///
    /// 即时操作，无需提交。
    Completed(usize),
    /// Async operation, needs to submit
    ///
    /// 异步操作，需要提交。
    Wait(WaitArg),
    /// Blocking operation, needs to be spawned in another thread
    ///
    /// 阻塞操作，需要孵化到阻塞线程中。
    Blocking,
    /// AIO operation, needs to be spawned to the kernel.
    ///
    /// AIO操作，需要孵化到内核中。
    #[cfg(aio)]
    Aio(AioControl),
}

impl Decision {
    /// Decide to wait for the given fd with the given interest.
    ///
    /// 创建一个提交决策：等待fd上发生关注的事件。
    pub fn wait_for(fd: RawFd, interest: Interest) -> Self {
        Self::Wait(WaitArg { fd, interest })
    }

    /// Decide to wait for the given fd to be readable.
    ///
    /// 创建一个提交决策：等待关注的可读(读就绪)事件。
    pub fn wait_readable(fd: RawFd) -> Self {
        Self::wait_for(fd, Interest::Readable)
    }

    /// Decide to wait for the given fd to be writable.
    ///
    /// 创建一个提交决策：等待关注的可写(写就绪)事件。
    pub fn wait_writable(fd: RawFd) -> Self {
        Self::wait_for(fd, Interest::Writable)
    }

    /// Decide to spawn an AIO operation. `submit` is a method like `aio_read`.
    ///
    /// 创建一个提交决策：孵化一个API操作。`submit`是一个类似`aio_read`的方法。
    #[cfg(aio)]
    pub fn aio(
        cb: &mut libc::aiocb,
        submit: unsafe extern "C" fn(*mut libc::aiocb) -> i32,
    ) -> Self {
        Self::Aio(AioControl {
            aiocbp: NonNull::from(cb),
            submit,
        })
    }
}

/// Meta of polling operations.
///
/// 提交给轮询框架的元数据：描述符，关注的事件。
#[derive(Debug, Clone, Copy)]
pub struct WaitArg {
    /// The raw fd of the operation.
    pub fd: RawFd,
    /// The interest to be registered.
    pub interest: Interest,
}

/// Meta of AIO operations.
///
/// AIO操作的元数据：???
#[cfg(aio)]
#[derive(Debug, Clone, Copy)]
pub struct AioControl {
    /// Pointer of the control block.
    pub aiocbp: NonNull<libc::aiocb>,
    /// The aio_* submit function.
    pub submit: unsafe extern "C" fn(*mut libc::aiocb) -> i32,
}

/// 记录某文件句柄关联的所有操作，并依据操作所关注的事件分配到2个队列中。
/// - 关注可读事件的操作，分配到读队列。
/// - 关注可写事件的操作，分配到写队列。
#[derive(Debug, Default)]
struct FdQueue {
    read_queue: VecDeque<usize>,
    write_queue: VecDeque<usize>,
}

impl FdQueue {
    /// 将操作编号，依据关注的事件，注册到描述符注册信息中的读或写队列尾部。
    pub fn push_back_interest(&mut self, user_data: usize, interest: Interest) {
        match interest {
            Interest::Readable => self.read_queue.push_back(user_data),
            Interest::Writable => self.write_queue.push_back(user_data),
        }
    }

    /// 将操作编号，依据关注的事件，注册到描述符注册信息中的读或写队列头部。
    pub fn push_front_interest(&mut self, user_data: usize, interest: Interest) {
        match interest {
            Interest::Readable => self.read_queue.push_front(user_data),
            Interest::Writable => self.write_queue.push_front(user_data),
        }
    }

    /// 从描述符注册信息中移除一个曾经关联过的操作编号。
    pub fn remove(&mut self, user_data: usize) {
        self.read_queue.retain(|&k| k != user_data);
        self.write_queue.retain(|&k| k != user_data);
    }

    /// 从描述符注册信息，构造描述符的事件请求对象。
    /// - 关注的事件：依据读和写队列只要非空就添加对应事件。
    /// - Token：优先选择写队列头部的操作编号，如果没有，
    ///   则选择读队列头部的操作编号。轮询到事件后，
    ///   会通过Token恢复出RawOp以获取到文件句柄。
    ///
    /// 为什么不直接用文件描述符做Token???
    pub fn event(&self) -> Event {
        let mut event = Event::none(0);
        if let Some(&key) = self.read_queue.front() {
            event.readable = true;
            event.key = key;
        }
        if let Some(&key) = self.write_queue.front() {
            event.writable = true;
            event.key = key;
        }
        event
    }

    /// 依据传入的事件类型(读就绪或写就绪)，
    /// 从对应的事件队列(读队列或写队列)弹出一个正在等待事件的操作。
    /// 优先处理读就绪。
    pub fn pop_interest(&mut self, event: &Event) -> Option<(usize, Interest)> {
        if event.readable {
            if let Some(user_data) = self.read_queue.pop_front() {
                return Some((user_data, Interest::Readable));
            }
        }
        if event.writable {
            if let Some(user_data) = self.write_queue.pop_front() {
                return Some((user_data, Interest::Writable));
            }
        }
        None
    }
}

/// Represents the filter type of kqueue. `polling` crate doesn't expose such
/// API, and we need to know about it when `cancel` is called.
///
/// 用于表示kqueue的过滤器类型。`polling`库没有提供这个API，
/// 但是当调用`cancel`时，需要知道它。
#[non_exhaustive]
pub enum OpType {
    /// The operation polls an fd.
    ///
    /// 操作轮询了一个FD。
    Fd(RawFd),
    /// The operation submits an AIO.
    ///
    /// 操作提交了一个AIO。
    #[cfg(aio)]
    Aio(NonNull<libc::aiocb>),
}

/// Low-level driver of polling.
///
/// Poll模式下的驱动器。
pub(crate) struct Driver {
    /// 接收轮训到的事件。
    events: Events,
    /// 使用的轮询器。
    poll: Arc<Poller>,
    /// 描述符关注事件注册表。
    registry: HashMap<RawFd, FdQueue>,
    /// 阻塞线程池。用于调度执行阻塞任务。
    pool: AsyncifyPool,
    /// 阻塞任务完结队列。可能执行完毕也可能被取消等等。
    pool_completed: Arc<SegQueue<Entry>>,
}

impl Driver {
    pub fn new(builder: &ProactorBuilder) -> io::Result<Self> {
        instrument!(compio_log::Level::TRACE, "new", ?builder);
        trace!("new poll driver");
        let entries = builder.capacity as usize; // for the sake of consistency, use u32 like iour
        let events = if entries == 0 {
            Events::new()
        } else {
            Events::with_capacity(NonZeroUsize::new(entries).unwrap())
        };

        let poll = Arc::new(Poller::new()?);

        Ok(Self {
            events,
            poll,
            registry: HashMap::new(),
            pool: builder.create_or_get_thread_pool(),
            pool_completed: Arc::new(SegQueue::new()),
        })
    }

    /// 创建一个操作信息块，返回其引用句柄。
    pub fn create_op<T: crate::sys::OpCode + 'static>(&self, op: T) -> Key<T> {
        Key::new(self.as_raw_fd(), op)
    }

    /// 提交某操作关注的事件，实际是提交了描述符正在关注的所有事件。
    /// - 1.先将操作关联的描述符，登记到驱动器的注册表中。(如果之间没登记过的话)
    /// - 2.从注册表中汇总描述符关注的所有事件(读和写)，全部同步给轮询器。
    ///
    /// 同步方式：如果描述符是首次注册，则使用add，否则使用modify。
    ///
    /// # Safety
    /// The input fd should be valid.
    ///
    /// # 安全性
    /// 输入的描述符必须是有效的。
    unsafe fn submit(&mut self, user_data: usize, arg: WaitArg) -> io::Result<()> {
        let need_add = !self.registry.contains_key(&arg.fd);
        let queue = self.registry.entry(arg.fd).or_default();
        queue.push_back_interest(user_data, arg.interest);
        let event = queue.event();

        // 如果描述符时首次添加，则add到轮询器。否则modify到轮询器。
        if need_add {
            self.poll.add(arg.fd, event)?;
        } else {
            let fd = BorrowedFd::borrow_raw(arg.fd);
            self.poll.modify(fd, event)?;
        }
        Ok(())
    }

    /// 依据提供的事件请求对象，刷新Poller和描述符注册表。
    /// - 如果无关注事件，则从描述符注册表和poller中删除描述符。
    /// - 否则，更新poller。
    fn renew(
        poll: &Poller,
        registry: &mut HashMap<RawFd, FdQueue>,
        fd: BorrowedFd,
        renew_event: Event,
    ) -> io::Result<()> {
        if !renew_event.readable && !renew_event.writable {
            poll.delete(fd)?;
            registry.remove(&fd.as_raw_fd());
        } else {
            poll.modify(fd, renew_event)?;
        }
        Ok(())
    }

    /// 没实现。
    pub fn attach(&mut self, _fd: RawFd) -> io::Result<()> {
        Ok(())
    }

    /// 任务取消
    /// - 从描述符注册表任务编号。
    /// - 依据注册表当前内容，构造请求事件对象，刷新poller和描述符注册表。
    /// - 向任务完成队列插入任务完成条目，任务结果标记为已取消。
    pub fn cancel(&mut self, op: &mut Key<dyn crate::sys::OpCode>) {
        let op_pin = op.as_op_pin();
        match op_pin.op_type() {
            None => {}
            Some(OpType::Fd(fd)) => {
                let queue = self
                    .registry
                    .get_mut(&fd)
                    .expect("the fd should be attached");
                queue.remove(op.user_data());
                let renew_event = queue.event();
                if Self::renew(
                    &self.poll,
                    &mut self.registry,
                    unsafe { BorrowedFd::borrow_raw(fd) },
                    renew_event,
                )
                .is_ok()
                {
                    self.pool_completed.push(entry_cancelled(op.user_data()));
                }
            }
            #[cfg(aio)]
            Some(OpType::Aio(aiocbp)) => {
                let aiocb = unsafe { aiocbp.as_ref() };
                let fd = aiocb.aio_fildes;
                syscall!(libc::aio_cancel(fd, aiocbp.as_ptr())).ok();
            }
        }
    }

    /// 依据操作当前预提交的决策结果，驱动操作的下一步处理。
    /// - Wait：等待。将操作正常提交给驱动程序，返回Pending。
    /// - Completed：完成。返回Ready。
    /// - Blocking：阻塞。驱动阻塞操作执行。
    pub fn push(&mut self, op: &mut Key<dyn crate::sys::OpCode>) -> Poll<io::Result<usize>> {
        instrument!(compio_log::Level::TRACE, "push", ?op);
        let user_data = op.user_data();
        let op_pin = op.as_op_pin();
        match op_pin.pre_submit()? {
            Decision::Wait(arg) => {
                // SAFETY: fd is from the OpCode.
                unsafe {
                    self.submit(user_data, arg)?;
                }
                trace!("register {:?}", arg);
                Poll::Pending
            }
            Decision::Completed(res) => Poll::Ready(Ok(res)),
            Decision::Blocking => self.push_blocking(user_data),
            #[cfg(aio)]
            Decision::Aio(AioControl { mut aiocbp, submit }) => {
                let aiocb = unsafe { aiocbp.as_mut() };
                #[cfg(freebsd)]
                {
                    // sigev_notify_kqueue
                    aiocb.aio_sigevent.sigev_signo = self.poll.as_raw_fd();
                    aiocb.aio_sigevent.sigev_notify = libc::SIGEV_KEVENT;
                    aiocb.aio_sigevent.sigev_value.sival_ptr = user_data as _;
                }
                #[cfg(solarish)]
                let mut notify = libc::port_notify {
                    portnfy_port: self.poll.as_raw_fd(),
                    portnfy_user: user_data as _,
                };
                #[cfg(solarish)]
                {
                    aiocb.aio_sigevent.sigev_notify = libc::SIGEV_PORT;
                    aiocb.aio_sigevent.sigev_value.sival_ptr = &mut notify as *mut _ as _;
                }
                match syscall!(submit(aiocbp.as_ptr())) {
                    Ok(_) => Poll::Pending,
                    // FreeBSD:
                    //   * EOPNOTSUPP: It's on a filesystem without AIO support. Just fallback to
                    //     blocking IO.
                    //   * EAGAIN: The process-wide queue is full. No safe way to remove the (maybe)
                    //     dead entries.
                    // Solarish:
                    //   * EAGAIN: Allocation failed.
                    Err(e)
                        if matches!(
                            e.raw_os_error(),
                            Some(libc::EOPNOTSUPP) | Some(libc::EAGAIN)
                        ) =>
                    {
                        self.push_blocking(user_data)
                    }
                    Err(e) => Poll::Ready(Err(e)),
                }
            }
        }
    }

    /// 驱动阻塞操作执行
    fn push_blocking(&mut self, user_data: usize) -> Poll<io::Result<usize>> {
        let poll = self.poll.clone();
        let completed = self.pool_completed.clone();
        // 这个闭包为阻塞操作的执行附加一些外壳逻辑。
        let mut closure = move || {
            // 通过user_data所表示的地址中的操作信息块，创建句柄。
            let mut op = unsafe { Key::<dyn crate::sys::OpCode>::new_unchecked(user_data) };
            // 提取并定住操作信息块中的操作对象。
            let op_pin = op.as_op_pin();
            // 执行操作。
            let res = match op_pin.operate() {
                Poll::Pending => unreachable!("this operation is not non-blocking"),
                Poll::Ready(res) => res,
            };
            // 存储操作结果。
            completed.push(Entry::new(user_data, res));
            // 唤醒驱动线程。
            poll.notify().ok();
        };

        // 将阻塞操作闭包分发给阻塞线程池进行执行。
        // 如果线程数超过上限，会报错，这时需要重新风阀。
        // 同时对于完成的操作做一次任务唤醒。
        loop {
            match self.pool.dispatch(closure) {
                Ok(()) => return Poll::Pending,
                Err(e) => {
                    closure = e.0;
                    self.poll_blocking();
                }
            }
        }
    }

    /// 轮询是否有阻塞操作完成。
    /// - 如果没有，则返回false。
    /// - 如果有，则全部弹出并唤醒所属的异步任务。
    fn poll_blocking(&mut self) -> bool {
        if self.pool_completed.is_empty() {
            return false;
        }
        while let Some(entry) = self.pool_completed.pop() {
            unsafe {
                entry.notify();
            }
        }
        true
    }

    /// 驱动IO框架。
    /// - 1.遍历阻塞任务完成队列，如果有完成，则全部处理并唤醒相关任务。
    ///   直接返回<-。
    /// - 2.等待IO框架有关注的事件发生。
    /// - 3.遍历所有发生的事件
    ///   - 根据key=user_data获取到事件对应的fd。。(为什么不直接用fd做key呢?)
    ///   - 根据fd从描述符注册表中查询到所有与就绪事件相关的操作，并执行。
    ///   - 每个操作执行完后，都唤醒自己所属的异步任务。
    ///     (处理每个就绪事件逻辑链路过长，不会影响实时性吗?)
    pub unsafe fn poll(&mut self, timeout: Option<Duration>) -> io::Result<()> {
        instrument!(compio_log::Level::TRACE, "poll", ?timeout);
        if self.poll_blocking() {
            return Ok(());
        }
        self.events.clear();
        self.poll.wait(&mut self.events, timeout)?;
        if self.events.is_empty() && timeout.is_some() {
            return Err(io::Error::from_raw_os_error(libc::ETIMEDOUT));
        }
        for event in self.events.iter() {
            // 这里的user_data只是当时为了做event的token从某个fd的注册表中随便选的其中一个。
            // 目的就是通过user_data恢复出操作对象，最终从操作对象中找出event对应的fd。
            //
            // 查找流程：
            // user_data --> Key<dyn OpCode> --> &mut dyn OpCode --> Fd --> FdQueue ->
            // user_data
            let user_data = event.key;
            trace!("receive {} for {:?}", user_data, event);
            let mut op = Key::<dyn crate::sys::OpCode>::new_unchecked(user_data);
            let op = op.as_op_pin();

            // 网络的Op类型都是在实现OpCode时写死的Some(Fd)，为什么会出现类型为None的Op呢？
            // 难道是RawOp被销毁后，再恢复出来的RawOp对象取类型会是None。
            match op.op_type() {
                None => {
                    // On epoll, multiple event may be received even if it is registered as
                    // one-shot. It is safe to ignore it.
                    trace!("op {} is completed", user_data);
                }
                Some(OpType::Fd(fd)) => {
                    // If it's an FD op, the returned user_data is only for calling `op_type`. We
                    // need to pop the real user_data from the queue.
                    let queue = self
                        .registry
                        .get_mut(&fd)
                        .expect("the fd should be attached");

                    // 问题：
                    // 1.这里如果event中同时包含读和写就绪，就只处理了读就绪事件？
                    // 那写就绪事件就不处理了？
                    // 2.实际的读写竟然放在了驱动轮询的过程中，
                    // 这样不会导致读写操作拖长了轮询的及时性了吗？
                    if let Some((user_data, interest)) = queue.pop_interest(&event) {
                        let mut op = Key::<dyn crate::sys::OpCode>::new_unchecked(user_data);
                        let op = op.as_op_pin();
                        let res = match op.operate() {
                            Poll::Pending => {
                                // The operation should go back to the front.
                                queue.push_front_interest(user_data, interest);
                                None
                            }
                            Poll::Ready(res) => Some(res),
                        };
                        if let Some(res) = res {
                            Entry::new(user_data, res).notify();
                        }
                    }
                    let renew_event = queue.event();
                    Self::renew(
                        &self.poll,
                        &mut self.registry,
                        BorrowedFd::borrow_raw(fd),
                        renew_event,
                    )?;
                }
                #[cfg(aio)]
                Some(OpType::Aio(aiocbp)) => {
                    let err = unsafe { libc::aio_error(aiocbp.as_ptr()) };
                    let res = match err {
                        // If the user_data is reused but the previously registered event still
                        // emits (for example, HUP in epoll; however it is impossible now
                        // because we only use AIO on FreeBSD), we'd better ignore the current
                        // one and wait for the real event.
                        libc::EINPROGRESS => {
                            trace!("op {} is not completed", user_data);
                            continue;
                        }
                        libc::ECANCELED => {
                            // Remove the aiocb from kqueue.
                            libc::aio_return(aiocbp.as_ptr());
                            Err(io::Error::from_raw_os_error(libc::ETIMEDOUT))
                        }
                        _ => syscall!(libc::aio_return(aiocbp.as_ptr())).map(|res| res as usize),
                    };
                    Entry::new(user_data, res).notify();
                }
            }
        }
        Ok(())
    }

    /// 创建驱动线程唤醒句柄
    pub fn handle(&self) -> NotifyHandle {
        NotifyHandle::new(self.poll.clone())
    }

    /// 创建缓冲池
    ///
    /// 对于轮询风格的驱动，使用后备缓冲池。
    /// - 缓冲块数量：buffer_len的下一个2次幂数值，例如3则取4(2^2),6则取8(2^3),
    ///   10则取16(2^4)。
    /// - 缓冲块长度：buffer_size。
    /// - 后备缓冲池的类型：VecDqueue<Vec<u8>>。
    pub fn create_buffer_pool(
        &mut self,
        buffer_len: u16,
        buffer_size: usize,
    ) -> io::Result<BufferPool> {
        #[cfg(fusion)]
        {
            Ok(BufferPool::new_poll(crate::FallbackBufferPool::new(
                buffer_len,
                buffer_size,
            )))
        }
        #[cfg(not(fusion))]
        {
            Ok(BufferPool::new(buffer_len, buffer_size))
        }
    }

    /// 释放缓冲池。
    /// # Safety
    ///
    /// caller must make sure release the buffer pool with correct driver
    pub unsafe fn release_buffer_pool(&mut self, _: BufferPool) -> io::Result<()> {
        Ok(())
    }
}

impl AsRawFd for Driver {
    fn as_raw_fd(&self) -> RawFd {
        self.poll.as_raw_fd()
    }
}

impl Drop for Driver {
    fn drop(&mut self) {
        for fd in self.registry.keys() {
            unsafe {
                let fd = BorrowedFd::borrow_raw(*fd);
                self.poll.delete(fd).ok();
            }
        }
    }
}

/// 创建操作结果条目，表明操作被取消以及取消原因。
fn entry_cancelled(user_data: usize) -> Entry {
    Entry::new(
        user_data,
        Err(io::Error::from_raw_os_error(libc::ETIMEDOUT)),
    )
}

/// A notify handle to the inner driver.
///
/// 驱动线程唤醒器句柄。持有轮询器的一个计数引用。
pub struct NotifyHandle {
    poll: Arc<Poller>,
}

impl NotifyHandle {
    fn new(poll: Arc<Poller>) -> Self {
        Self { poll }
    }

    /// Notify the inner driver.
    ///
    /// 唤醒驱动器线程阻塞等待操作。
    pub fn notify(&self) -> io::Result<()> {
        self.poll.notify()
    }
}
