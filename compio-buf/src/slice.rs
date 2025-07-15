use std::ops::{Deref, DerefMut};

use crate::*;

/// An owned view into a contiguous sequence of bytes.
///
/// 连续字节的所有权视图。
///
/// This is similar to Rust slices (`&buf[..]`) but owns the underlying buffer.
/// This type is useful for performing io-uring read and write operations using
/// a subset of a buffer.
///
/// 类似切片(`&buf[..]`)，但是并非引用形式。
/// 由于io_uring的read和write需要将缓冲区移入操作，
/// 因此，可以用在执行io_uring的read和write时，使用缓冲区的子集。
/// 
/// 注意：
/// T不是元素类型，而是底层缓冲区类型。
///
/// Slices are created using [`IoBuf::slice`].
///
/// # Examples
///
/// Creating a slice
///
/// ```
/// use compio_buf::IoBuf;
///
/// let buf = b"hello world";
/// let slice = buf.slice(..5);
///
/// assert_eq!(&slice[..], b"hello");
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Slice<T> {
    buffer: T,
    begin: usize,
    end: usize,
}

impl<T> Slice<T> {
    pub(crate) fn new(buffer: T, begin: usize, end: usize) -> Self {
        Self { buffer, begin, end }
    }

    /// Offset in the underlying buffer at which this slice starts.
    ///
    /// 切片在底层缓冲区的起点。
    pub fn begin(&self) -> usize {
        self.begin
    }

    /// Offset in the underlying buffer at which this slice ends.
    ///
    /// 切片在底层缓冲区的重终点。
    pub fn end(&self) -> usize {
        self.end
    }

    /// 同时设置起点和终点。
    pub(crate) fn set_range(&mut self, begin: usize, end: usize) {
        self.begin = begin;
        self.end = end;
    }

    /// Gets a reference to the underlying buffer.
    ///
    /// 获取底层缓冲区的引用，通过此方法脱离切片视图。
    ///
    /// This method escapes the slice's view.
    pub fn as_inner(&self) -> &T {
        &self.buffer
    }

    /// Gets a mutable reference to the underlying buffer.
    ///
    /// 获取底层缓冲区的可变引用。
    ///
    /// This method escapes the slice's view.
    pub fn as_inner_mut(&mut self) -> &mut T {
        &mut self.buffer
    }
}

/// 将缓冲区引用转为字节切片。
fn slice_mut<T: IoBufMut>(buffer: &mut T) -> &mut [u8] {
    unsafe { std::slice::from_raw_parts_mut(buffer.as_buf_mut_ptr(), (*buffer).buf_len()) }
}

/// 所有权切片解引用为字节切片。
impl<T: IoBuf> Deref for Slice<T> {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        let bytes = self.buffer.as_slice();
        let end = self.end.min(bytes.len());
        &bytes[self.begin..end]
    }
}

impl<T: IoBufMut> DerefMut for Slice<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        let bytes = slice_mut(&mut self.buffer);
        let end = self.end.min(bytes.len());
        &mut bytes[self.begin..end]
    }
}

/// 如果底层缓冲区实现了缓冲器接口，则位于其上的所有权切片Slice<T>也自动实现。
unsafe impl<T: IoBuf> IoBuf for Slice<T> {
    fn as_buf_ptr(&self) -> *const u8 {
        self.buffer.as_slice()[self.begin..].as_ptr()
    }

    fn buf_len(&self) -> usize {
        self.deref().len()
    }

    fn buf_capacity(&self) -> usize {
        self.end - self.begin
    }
}

unsafe impl<T: IoBufMut> IoBufMut for Slice<T> {
    fn as_buf_mut_ptr(&mut self) -> *mut u8 {
        slice_mut(&mut self.buffer)[self.begin..].as_mut_ptr()
    }
}

impl<T: SetBufInit> SetBufInit for Slice<T> {
    unsafe fn set_buf_init(&mut self, len: usize) {
        self.buffer.set_buf_init(self.begin + len)
    }
}

impl<T> IntoInner for Slice<T> {
    type Inner = T;

    fn into_inner(self) -> Self::Inner {
        self.buffer
    }
}
