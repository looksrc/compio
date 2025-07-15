#[cfg_attr(all(doc, docsrs), doc(cfg(all())))]
#[allow(unused_imports)]
pub use std::os::fd::{AsFd, AsRawFd, BorrowedFd, OwnedFd, RawFd};
use std::{io, os::fd::FromRawFd, pin::Pin, sync::Arc, task::Poll, time::Duration};

use compio_log::{instrument, trace, warn};
use crossbeam_queue::SegQueue;
cfg_if::cfg_if! {
    if #[cfg(feature = "io-uring-cqe32")] {
        use io_uring::cqueue::Entry32 as CEntry;
    } else {
        use io_uring::cqueue::Entry as CEntry;
    }
}
cfg_if::cfg_if! {
    if #[cfg(feature = "io-uring-sqe128")] {
        use io_uring::squeue::Entry128 as SEntry;
    } else {
        use io_uring::squeue::Entry as SEntry;
    }
}
use io_uring::{
    IoUring,
    cqueue::more,
    opcode::{AsyncCancel, PollAdd},
    types::{Fd, SubmitArgs, Timespec},
};
pub(crate) use libc::{sockaddr_storage, socklen_t};
#[cfg(io_uring)]
use slab::Slab;

use crate::{AsyncifyPool, BufferPool, Entry, Key, ProactorBuilder, syscall};

pub(crate) mod op;

/// The created entry of [`OpCode`].
pub enum OpEntry {
    /// This operation creates an io-uring submission entry.
    Submission(io_uring::squeue::Entry),
    #[cfg(feature = "io-uring-sqe128")]
    /// This operation creates an 128-bit io-uring submission entry.
    Submission128(io_uring::squeue::Entry128),
    /// This operation is a blocking one.
    Blocking,
}

impl From<io_uring::squeue::Entry> for OpEntry {
    fn from(value: io_uring::squeue::Entry) -> Self {
        Self::Submission(value)
    }
}

#[cfg(feature = "io-uring-sqe128")]
impl From<io_uring::squeue::Entry128> for OpEntry {
    fn from(value: io_uring::squeue::Entry128) -> Self {
        Self::Submission128(value)
    }
}

/// Abstraction of io-uring operations.
///
/// IoUring操作码接口。在IoUring驱动中执行的操作码必须实现此接口。
pub trait OpCode {
    /// Create submission entry.
    fn create_entry(self: Pin<&mut Self>) -> OpEntry;

    /// Call the operation in a blocking way. This method will only be called if
    /// [`create_entry`] returns [`OpEntry::Blocking`].
    fn call_blocking(self: Pin<&mut Self>) -> io::Result<usize> {
        unreachable!("this operation is asynchronous")
    }

    /// Set the result when it successfully completes.
    /// The operation stores the result and is responsible to release it if the
    /// operation is cancelled.
    ///
    /// # Safety
    ///
    /// Users should not call it.
    unsafe fn set_result(self: Pin<&mut Self>, _: usize) {}
}

/// Low-level driver of io-uring.
///
/// io-uring类型的驱动器。
pub(crate) struct Driver {
    /// IoUring实例。
    inner: IoUring<SEntry, CEntry>,
    /// 驱动线程唤醒器。
    notifier: Notifier,
    /// 阻塞线程池。
    pool: AsyncifyPool,
    pool_completed: Arc<SegQueue<Entry>>,
    /// IoUring缓冲表，记录所有创建的环形缓冲区。
    #[cfg(io_uring)]
    buffer_group_ids: Slab<()>,
}

impl Driver {
    /// 取消操作的操作编号。比如AsyncCancel。
    const CANCEL: u64 = u64::MAX;
    /// eventfd的Read操作编号，用于唤醒正在阻塞等待的驱动器线程。
    const NOTIFY: u64 = u64::MAX - 1;

    /// 依据传入的构建器构建一个io-uring驱动器
    /// - 1.创建驱动线程唤醒器，并向io-uring提交对其的读就绪监控。
    /// - 2.设置sqpoll标记，确定是否开启sq自动提交。
    /// - 3.设置coop_taskrun标记，当产生了CQE时，会向用户空间发送中断信号。
    /// - 4.设置taskrun_flag标记，可通过此标记判定系统是否产生了CQE。
    /// - 5.设置队列条目上限。
    pub fn new(builder: &ProactorBuilder) -> io::Result<Self> {
        instrument!(compio_log::Level::TRACE, "new", ?builder);
        trace!("new iour driver");
        let notifier = Notifier::new()?;
        let mut io_uring_builder = IoUring::builder();
        if let Some(sqpoll_idle) = builder.sqpoll_idle {
            io_uring_builder.setup_sqpoll(sqpoll_idle.as_millis() as _);
        }
        if builder.coop_taskrun {
            io_uring_builder.setup_coop_taskrun();
        }
        if builder.taskrun_flag {
            io_uring_builder.setup_taskrun_flag();
        }

        let mut inner = io_uring_builder.build(builder.capacity)?;
        #[allow(clippy::useless_conversion)]
        unsafe {
            inner
                .submission()
                .push(
                    &PollAdd::new(Fd(notifier.as_raw_fd()), libc::POLLIN as _)
                        .multi(true)
                        .build()
                        .user_data(Self::NOTIFY)
                        .into(),
                )
                .expect("the squeue sould not be full");
        }
        Ok(Self {
            inner,
            notifier,
            pool: builder.create_or_get_thread_pool(),
            pool_completed: Arc::new(SegQueue::new()),
            #[cfg(io_uring)]
            buffer_group_ids: Slab::new(),
        })
    }

    /// Auto means that it choose to wait or not automatically.
    ///
    /// 依据当前CQE的情况，来自动确定提交SQ的方式
    /// - 1.如果有CQE在等待处理，则以非阻塞方式提交。
    /// - 2.否则，以阻塞方式提交，此时设置阻塞超时时间。
    fn submit_auto(&mut self, timeout: Option<Duration>) -> io::Result<()> {
        instrument!(compio_log::Level::TRACE, "submit_auto", ?timeout);

        // when taskrun is true, there are completed cqes wait to handle, no need to
        // block the submit
        // 当有CQE时，SQ提交操作不应当阻塞。
        // 因此用want_sqe记录想要阻塞等待的CQE数量，数量为0标明submit无需等待(阻塞)。
        let want_sqe = if self.inner.submission().taskrun() {
            0
        } else {
            1
        };

        // 提交SQ，同时提交一个超时操作，防止提交操作在设置了want_sqe时一直阻塞。
        let res = {
            // Last part of submission queue, wait till timeout.
            if let Some(duration) = timeout {
                let timespec = timespec(duration);
                let args = SubmitArgs::new().timespec(&timespec);
                self.inner.submitter().submit_with_args(want_sqe, &args)
            } else {
                self.inner.submit_and_wait(want_sqe)
            }
        };

        // 处理提交SQ的结果，(在允许阻塞情况下，执行到此步表明要么等待超时了，
        // 要么有任务完成了)
        // - 1.如果没有任务完成，说明超时了，返回超时错误。
        // - 2.如果提交操作报错了，则返回响相应的操作系统错误。
        trace!("submit result: {res:?}");
        match res {
            Ok(_) => {
                if self.inner.completion().is_empty() {
                    Err(io::ErrorKind::TimedOut.into())
                } else {
                    Ok(())
                }
            }
            Err(e) => match e.raw_os_error() {
                Some(libc::ETIME) => Err(io::ErrorKind::TimedOut.into()),
                Some(libc::EBUSY) | Some(libc::EAGAIN) => Err(io::ErrorKind::Interrupted.into()),
                _ => Err(e),
            },
        }
    }

    /// 轮询阻塞操作结果队列，唤醒异步操作所属的异步任务。
    fn poll_blocking(&mut self) {
        // Cheaper than pop.
        if !self.pool_completed.is_empty() {
            while let Some(entry) = self.pool_completed.pop() {
                unsafe {
                    entry.notify();
                }
            }
        }
    }

    /// 轮询所有操作结果，唤醒异步操作所属的异步任务。
    /// - 1.先轮询阻塞操作结果。
    /// - 2.轮询CQ中俄结果。
    ///
    /// 返回值：
    /// - true：CQ非空，本轮轮询到了CQ任务。
    /// - false：CQ为空，本轮没有轮询到CQ任务。
    ///
    /// CQE分类：
    /// - CANCEL：取消操作的CQE，不需要后续处理。
    /// - NOTIFY：通知线程解除阻塞的的CQE，即eventfd的read操作完成，
    ///   还原eventfd状态。
    /// - 其它：将操作结果设置给RawOp，并唤醒Op所在的任务。
    fn poll_entries(&mut self) -> bool {
        self.poll_blocking();

        let mut cqueue = self.inner.completion();
        cqueue.sync();
        let has_entry = !cqueue.is_empty();
        for entry in cqueue {
            match entry.user_data() {
                Self::CANCEL => {}
                Self::NOTIFY => {
                    let flags = entry.flags();
                    debug_assert!(more(flags));
                    self.notifier.clear().expect("cannot clear notifier");
                }
                _ => unsafe {
                    create_entry(entry).notify();
                },
            }
        }
        has_entry
    }

    /// 创建一个异步操作对象
    pub fn create_op<T: crate::sys::OpCode + 'static>(&self, op: T) -> Key<T> {
        Key::new(self.as_raw_fd(), op)
    }

    pub fn attach(&mut self, _fd: RawFd) -> io::Result<()> {
        Ok(())
    }

    pub fn cancel(&mut self, op: &mut Key<dyn crate::sys::OpCode>) {
        instrument!(compio_log::Level::TRACE, "cancel", ?op);
        trace!("cancel RawOp");
        unsafe {
            #[allow(clippy::useless_conversion)]
            if self
                .inner
                .submission()
                .push(
                    &AsyncCancel::new(op.user_data() as _)
                        .build()
                        .user_data(Self::CANCEL)
                        .into(),
                )
                .is_err()
            {
                warn!("could not push AsyncCancel entry");
            }
        }
    }

    fn push_raw(&mut self, entry: SEntry) -> io::Result<()> {
        loop {
            let mut squeue = self.inner.submission();
            match unsafe { squeue.push(&entry) } {
                Ok(()) => {
                    squeue.sync();
                    break Ok(());
                }
                Err(_) => {
                    drop(squeue);
                    self.poll_entries();
                    match self.submit_auto(Some(Duration::ZERO)) {
                        Ok(()) => {}
                        Err(e)
                            if matches!(
                                e.kind(),
                                io::ErrorKind::TimedOut | io::ErrorKind::Interrupted
                            ) => {}
                        Err(e) => return Err(e),
                    }
                }
            }
        }
    }

    pub fn push(&mut self, op: &mut Key<dyn crate::sys::OpCode>) -> Poll<io::Result<usize>> {
        instrument!(compio_log::Level::TRACE, "push", ?op);
        let user_data = op.user_data();
        let op_pin = op.as_op_pin();
        trace!("push RawOp");
        match op_pin.create_entry() {
            OpEntry::Submission(entry) => {
                #[allow(clippy::useless_conversion)]
                self.push_raw(entry.user_data(user_data as _).into())?;
                Poll::Pending
            }
            #[cfg(feature = "io-uring-sqe128")]
            OpEntry::Submission128(entry) => {
                self.push_raw(entry.user_data(user_data as _))?;
                Poll::Pending
            }
            OpEntry::Blocking => loop {
                if self.push_blocking(user_data) {
                    break Poll::Pending;
                } else {
                    self.poll_blocking();
                }
            },
        }
    }

    /// 利用阻塞线程池执行一个阻塞操作。
    ///
    /// 线程池任务流程：
    /// - 对阻塞操作进行包装，
    /// - 阻塞操作完成后插入任务完成队列。
    /// - 唤醒驱动器线程。
    fn push_blocking(&mut self, user_data: usize) -> bool {
        // 驱动线程唤醒器
        let handle = self.handle();
        let completed = self.pool_completed.clone();
        // 向阻塞线程池发一个任务
        self.pool
            .dispatch(move || {
                let mut op = unsafe { Key::<dyn crate::sys::OpCode>::new_unchecked(user_data) };
                let op_pin = op.as_op_pin();
                let res = op_pin.call_blocking();
                completed.push(Entry::new(user_data, res));
                handle.notify().ok();
            })
            .is_ok()
    }

    pub unsafe fn poll(&mut self, timeout: Option<Duration>) -> io::Result<()> {
        instrument!(compio_log::Level::TRACE, "poll", ?timeout);
        // Anyway we need to submit once, no matter there are entries in squeue.
        trace!("start polling");

        if !self.poll_entries() {
            self.submit_auto(timeout)?;
            self.poll_entries();
        }

        Ok(())
    }

    /// 获取驱动器线程唤醒句柄。
    pub fn handle(&self) -> NotifyHandle {
        self.notifier.handle()
    }

    /// 创建缓冲池，IoUring
    /// - 从缓冲池列表slab中找空闲槽位，获取槽位编号作为新缓冲池的buf_group。
    /// - 新建一个与IoUring关联的环形缓冲池，使用上面已确定的buf_group。
    /// -
    #[cfg(io_uring)]
    pub fn create_buffer_pool(
        &mut self,
        buffer_len: u16,
        buffer_size: usize,
    ) -> io::Result<BufferPool> {
        let buffer_group = self.buffer_group_ids.insert(());
        if buffer_group > u16::MAX as usize {
            self.buffer_group_ids.remove(buffer_group);

            return Err(io::Error::new(
                io::ErrorKind::OutOfMemory,
                "too many buffer pool allocated",
            ));
        }

        let buf_ring = io_uring_buf_ring::IoUringBufRing::new(
            &self.inner,
            buffer_len,
            buffer_group as _,
            buffer_size,
        )?;

        // 融合缓冲池
        #[cfg(fusion)]
        {
            Ok(BufferPool::new_io_uring(crate::IoUringBufferPool::new(
                buf_ring,
            )))
        }

        // 非融合缓冲池
        #[cfg(not(fusion))]
        {
            Ok(BufferPool::new(buf_ring))
        }
    }

    /// 创建缓冲池。非IoUring。实际是一个Vec<u8>作为缓冲块的VecQueue队列。
    #[cfg(not(io_uring))]
    pub fn create_buffer_pool(
        &mut self,
        buffer_len: u16,
        buffer_size: usize,
    ) -> io::Result<BufferPool> {
        Ok(BufferPool::new(buffer_len, buffer_size))
    }

    /// 释放缓冲池，iouring。
    /// - 从IoUring中释放环形缓冲池。
    /// - 从驱动器的缓冲表中删除被释放的缓冲池编号。
    ///
    /// # Safety
    ///
    /// caller must make sure release the buffer pool with correct driver
    #[cfg(io_uring)]
    pub unsafe fn release_buffer_pool(&mut self, buffer_pool: BufferPool) -> io::Result<()> {
        #[cfg(fusion)]
        let buffer_pool = buffer_pool.into_io_uring();

        let buffer_group = buffer_pool.buffer_group();
        buffer_pool.into_inner().release(&self.inner)?;
        self.buffer_group_ids.remove(buffer_group as _);

        Ok(())
    }

    /// 释放缓冲池，非iouring，直接将缓冲池遗弃。
    /// # Safety
    ///
    /// caller must make sure release the buffer pool with correct driver
    #[cfg(not(io_uring))]
    pub unsafe fn release_buffer_pool(&mut self, _: BufferPool) -> io::Result<()> {
        Ok(())
    }
}

impl AsRawFd for Driver {
    fn as_raw_fd(&self) -> RawFd {
        self.inner.as_raw_fd()
    }
}

/// 将io_uring包中的CQ Entry转换为本工程自己写的 Entry。
///
/// 主要是转换result，其它内容直接原样复制
/// - 如果result=-libc::ECANCELED，替换为libc::ETIMEDOUT。
/// - 将result的数值，转换为Result<T,E>，错误从操作系统中提取。
fn create_entry(cq_entry: CEntry) -> Entry {
    let result = cq_entry.result();
    let result = if result < 0 {
        let result = if result == -libc::ECANCELED {
            libc::ETIMEDOUT
        } else {
            -result
        };
        Err(io::Error::from_raw_os_error(result))
    } else {
        Ok(result as _)
    };
    let mut entry = Entry::new(cq_entry.user_data() as _, result);
    entry.set_flags(cq_entry.flags());

    entry
}

/// 将Rust中的时间跨度Duration转换为C中使用的Timespc格式，用于调用C函数。
fn timespec(duration: std::time::Duration) -> Timespec {
    Timespec::new()
        .sec(duration.as_secs())
        .nsec(duration.subsec_nanos())
}

/// 驱动线程唤醒器，一般利用eventfd的可读事件实现驱动唤醒。
#[derive(Debug)]
struct Notifier {
    fd: Arc<OwnedFd>,
}

impl Notifier {
    /// Create a new notifier.
    ///
    /// 创建一个驱动线程唤醒器。本质为一个eventfd描述符的包装。
    fn new() -> io::Result<Self> {
        let fd = syscall!(libc::eventfd(0, libc::EFD_CLOEXEC | libc::EFD_NONBLOCK))?;
        let fd = unsafe { OwnedFd::from_raw_fd(fd) };
        Ok(Self { fd: Arc::new(fd) })
    }

    /// 通过读取操作将eventfd置空。返回Ok表明已经置空。
    ///
    /// 不同的读取结果处理：
    /// - 正常读取，则正常置空了，置空成功。
    /// - 无物可读，表明已经为空，置空成功。
    /// - 读取报错，置空失败，返回Err(e)。
    /// - 读取中断，再次尝试读取。
    pub fn clear(&self) -> io::Result<()> {
        loop {
            let mut buffer = [0u64];
            let res = syscall!(libc::read(
                self.fd.as_raw_fd(),
                buffer.as_mut_ptr().cast(),
                std::mem::size_of::<u64>()
            ));
            match res {
                Ok(len) => {
                    debug_assert_eq!(len, std::mem::size_of::<u64>() as _);
                    break Ok(());
                }
                // Clear the next time:)
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => break Ok(()),
                // Just like read_exact
                Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
                Err(e) => break Err(e),
            }
        }
    }

    /// 从通知器构建驱动器线程唤醒句柄。
    pub fn handle(&self) -> NotifyHandle {
        NotifyHandle::new(self.fd.clone())
    }
}

impl AsRawFd for Notifier {
    fn as_raw_fd(&self) -> RawFd {
        self.fd.as_raw_fd()
    }
}

/// A notify handle to the inner driver.
///
/// iouring驱动器的唤醒句柄。
///
/// 通知方式：
/// - 被通知方：监控eventfd的可读事件，一旦收到可读事件就解除阻塞。
/// - 通知方：向eventfd写入内容，触发它的可读事件。
pub struct NotifyHandle {
    fd: Arc<OwnedFd>,
}

impl NotifyHandle {
    pub(crate) fn new(fd: Arc<OwnedFd>) -> Self {
        Self { fd }
    }

    /// Notify the inner driver.
    ///
    /// 向eventfd写入内容，触发可读事件，以此通知iouring驱动解除阻塞。
    pub fn notify(&self) -> io::Result<()> {
        let data = 1u64;
        syscall!(libc::write(
            self.fd.as_raw_fd(),
            &data as *const _ as *const _,
            std::mem::size_of::<u64>(),
        ))?;
        Ok(())
    }
}
