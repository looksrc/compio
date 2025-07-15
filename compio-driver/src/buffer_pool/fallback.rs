//! The fallback buffer pool. It is backed by a [`VecDeque`] of [`Vec<u8>`].
//! An [`OwnedBuffer`] is selected when the op is created. It keeps a strong
//! reference to the buffer pool. The [`BorrowedBuffer`] is created after the op
//! returns successfully.

use std::{
    borrow::{Borrow, BorrowMut},
    cell::RefCell,
    collections::VecDeque,
    fmt::{Debug, Formatter},
    io,
    mem::ManuallyDrop,
    ops::{Deref, DerefMut},
    rc::Rc,
};

use compio_buf::{IntoInner, IoBuf, IoBufMut, SetBufInit, Slice};

struct BufferPoolInner {
    buffers: RefCell<VecDeque<Vec<u8>>>,
}

impl BufferPoolInner {
    pub(crate) fn add_buffer(&self, mut buffer: Vec<u8>) {
        buffer.clear();
        self.buffers.borrow_mut().push_back(buffer)
    }
}

/// Buffer pool
///
/// A buffer pool to allow user no need to specify a specific buffer to do the
/// IO operation
///
/// 缓冲池。用于非iouring驱动的操作。
///
/// 实现：
/// - 通过队列实现环形缓冲区。
/// - 使用时从队列弹出，所有权移交给使用者。
/// - 使用后压回队列尾部，所有权又回来了。
pub struct BufferPool {
    inner: Rc<BufferPoolInner>,
}

impl Debug for BufferPool {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BufferPool").finish_non_exhaustive()
    }
}

impl BufferPool {
    /// 创建缓冲池
    /// - 指定了缓冲块大小，以及缓冲块总数。
    /// - 缓冲池使用引用计数，内部可变。
    pub(crate) fn new(buffer_len: u16, buffer_size: usize) -> Self {
        // To match the behavior of io-uring, extend the number of buffers.
        let buffers = (0..buffer_len.next_power_of_two())
            .map(|_| Vec::with_capacity(buffer_size))
            .collect();

        Self {
            inner: Rc::new(BufferPoolInner {
                buffers: RefCell::new(buffers),
            }),
        }
    }

    /// Select an [`OwnedBuffer`] when the op creates.
    ///
    /// 弹出一个缓冲块，获取其所有权，按长度要求和容量对其进行修正，
    /// 取两者最小值。
    #[doc(hidden)]
    pub fn get_buffer(&self, len: usize) -> io::Result<OwnedBuffer> {
        let buffer = self
            .inner
            .buffers
            .borrow_mut()
            .pop_front()
            .ok_or_else(|| io::Error::other("buffer ring has no available buffer"))?;
        let len = if len == 0 {
            buffer.capacity()
        } else {
            buffer.capacity().min(len)
        };
        Ok(OwnedBuffer::new(buffer.slice(..len), self.inner.clone()))
    }

    /// Return the buffer to the pool.
    ///
    /// 添加一个缓冲块。
    pub(crate) fn add_buffer(&self, buffer: Vec<u8>) {
        self.inner.add_buffer(buffer);
    }

    /// 将具有所有权的缓冲块，转为借用缓冲块，同时按需求修正其长度。
    /// ## Safety
    /// * `len` should be valid.
    #[doc(hidden)]
    pub unsafe fn create_proxy(&self, mut slice: OwnedBuffer, len: usize) -> BorrowedBuffer<'_> {
        unsafe {
            slice.set_buf_init(len);
        }
        BorrowedBuffer::new(slice.into_inner(), self)
    }
}

/// 持有缓冲块所有权，缓冲池的引用计数。
///
/// drop逻辑：
/// - 缓冲坏还回缓冲池。
/// - 缓冲池减少引用计数。
#[doc(hidden)]
pub struct OwnedBuffer {
    buffer: ManuallyDrop<Slice<Vec<u8>>>,
    pool: ManuallyDrop<Rc<BufferPoolInner>>,
}

impl OwnedBuffer {
    fn new(buffer: Slice<Vec<u8>>, pool: Rc<BufferPoolInner>) -> Self {
        Self {
            buffer: ManuallyDrop::new(buffer),
            pool: ManuallyDrop::new(pool),
        }
    }
}

unsafe impl IoBuf for OwnedBuffer {
    fn as_buf_ptr(&self) -> *const u8 {
        self.buffer.as_buf_ptr()
    }

    fn buf_len(&self) -> usize {
        self.buffer.buf_len()
    }

    fn buf_capacity(&self) -> usize {
        self.buffer.buf_capacity()
    }
}

unsafe impl IoBufMut for OwnedBuffer {
    fn as_buf_mut_ptr(&mut self) -> *mut u8 {
        self.buffer.as_buf_mut_ptr()
    }
}

impl SetBufInit for OwnedBuffer {
    unsafe fn set_buf_init(&mut self, len: usize) {
        self.buffer.set_buf_init(len);
    }
}

impl Drop for OwnedBuffer {
    fn drop(&mut self) {
        // Safety: `take` is called only once here.
        self.pool
            .add_buffer(unsafe { ManuallyDrop::take(&mut self.buffer) }.into_inner());
        // Safety: `drop` is called only once here.
        unsafe { ManuallyDrop::drop(&mut self.pool) };
    }
}

impl IntoInner for OwnedBuffer {
    type Inner = Slice<Vec<u8>>;

    fn into_inner(mut self) -> Self::Inner {
        // Safety: `self` is forgotten in this method.
        let buffer = unsafe { ManuallyDrop::take(&mut self.buffer) };
        // The buffer is taken, we only need to drop the Rc.
        // Safety: `self` is forgotten in this method.
        unsafe { ManuallyDrop::drop(&mut self.pool) };
        std::mem::forget(self);
        buffer
    }
}

/// Buffer borrowed from buffer pool
///
/// When IO operation finish, user will obtain a `BorrowedBuffer` to access the
/// filled data
///
/// 缓冲块借用，持有缓冲块的所有权，持有缓冲池的只读引用
///
/// drop逻辑：
/// - 将缓冲块换回缓冲池。
pub struct BorrowedBuffer<'a> {
    buffer: ManuallyDrop<Slice<Vec<u8>>>,
    pool: &'a BufferPool,
}

impl<'a> BorrowedBuffer<'a> {
    pub(crate) fn new(buffer: Slice<Vec<u8>>, pool: &'a BufferPool) -> Self {
        Self {
            buffer: ManuallyDrop::new(buffer),
            pool,
        }
    }
}

impl Debug for BorrowedBuffer<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BorrowedBuffer").finish_non_exhaustive()
    }
}

impl Drop for BorrowedBuffer<'_> {
    fn drop(&mut self) {
        // Safety: `take` is called only once here.
        let buffer = unsafe { ManuallyDrop::take(&mut self.buffer) };
        self.pool.add_buffer(buffer.into_inner());
    }
}

impl Deref for BorrowedBuffer<'_> {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.buffer.deref()
    }
}

impl DerefMut for BorrowedBuffer<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.buffer.deref_mut()
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
