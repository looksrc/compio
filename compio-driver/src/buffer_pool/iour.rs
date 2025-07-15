//! The io-uring buffer pool. It is backed by a [`Vec`] of [`Vec<u8>`].
//! The kernel selects the buffer and returns `flags`. The crate
//! [`io_uring_buf_ring`] handles the returning of buffer on drop.

use std::{
    borrow::{Borrow, BorrowMut},
    fmt::{Debug, Formatter},
    io,
    ops::{Deref, DerefMut},
};

use io_uring::cqueue::buffer_select;
use io_uring_buf_ring::IoUringBufRing;

/// Buffer pool
///
/// 缓冲池，实际是IoUring中注册的环形缓冲池，套壳io_uring_buf_ring库。
/// - 通过buf_group引用缓冲池。
/// - 通过sqe中的buffer_select(cqe.flags)计算出所使用的缓冲块在池中的索引。
///
/// A buffer pool to allow user no need to specify a specific buffer to do the
/// IO operation
pub struct BufferPool {
    buf_ring: IoUringBufRing<Vec<u8>>,
}

impl Debug for BufferPool {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BufferPool").finish_non_exhaustive()
    }
}

impl BufferPool {
    /// 新建一个IoUring使用的缓冲池，实际是创建了一个和IoUring关联的环形缓冲池。
    pub(crate) fn new(buf_ring: IoUringBufRing<Vec<u8>>) -> Self {
        Self { buf_ring }
    }

    /// 唤醒缓冲区的编号，用于引用唤醒缓冲池。
    pub(crate) fn buffer_group(&self) -> u16 {
        self.buf_ring.buffer_group()
    }

    /// 取环形缓冲池的句柄。
    pub(crate) fn into_inner(self) -> IoUringBufRing<Vec<u8>> {
        self.buf_ring
    }

    /// 根据cqe的flags取出本次操作使用的缓冲块。
    /// - 1.根据buffer_select + flags，计算出buffer_id。
    /// - 2.通过get_buf + buffer_id，取得缓存块的借用。
    ///
    /// ## Safety
    /// * `available_len` should be the returned value from the op.
    pub(crate) unsafe fn get_buffer(
        &self,
        flags: u32,
        available_len: usize,
    ) -> io::Result<BorrowedBuffer<'_>> {
        let buffer_id = buffer_select(flags).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("flags {flags} is invalid"),
            )
        })?;

        self.buf_ring
            .get_buf(buffer_id, available_len)
            .map(BorrowedBuffer)
            .ok_or_else(|| io::Error::other(format!("cannot find buffer {buffer_id}")))
    }

    /// ???
    pub(crate) fn reuse_buffer(&self, flags: u32) {
        // It ignores invalid flags.
        if let Some(buffer_id) = buffer_select(flags) {
            // Safety: 0 is always valid length. We just want to get the buffer once and
            // return it immediately.
            unsafe { self.buf_ring.get_buf(buffer_id, 0) };
        }
    }
}

/// Buffer borrowed from buffer pool
///
/// 缓冲块引用：
/// - 操作完成后，用户需要通过`BorrowedBuffer`访问缓冲块中填充的数据。
/// - 引用被丢弃时，缓冲块会被重置，以备复用。
///
/// When IO operation finish, user will obtain a `BorrowedBuffer` to access the
/// filled data
pub struct BorrowedBuffer<'a>(io_uring_buf_ring::BorrowedBuffer<'a, Vec<u8>>);

impl Debug for BorrowedBuffer<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BorrowedBuffer").finish_non_exhaustive()
    }
}

impl Deref for BorrowedBuffer<'_> {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.0.deref()
    }
}

impl DerefMut for BorrowedBuffer<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.0.deref_mut()
    }
}

impl AsRef<[u8]> for BorrowedBuffer<'_> {
    fn as_ref(&self) -> &[u8] {
        self.deref()
    }
}

impl AsMut<[u8]> for BorrowedBuffer<'_> {
    fn as_mut(&mut self) -> &mut [u8] {
        self.deref_mut()
    }
}

impl Borrow<[u8]> for BorrowedBuffer<'_> {
    fn borrow(&self) -> &[u8] {
        self.deref()
    }
}

impl BorrowMut<[u8]> for BorrowedBuffer<'_> {
    fn borrow_mut(&mut self) -> &mut [u8] {
        self.deref_mut()
    }
}
