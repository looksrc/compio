cfg_if::cfg_if! {
    if #[cfg(io_uring)] {
        cfg_if::cfg_if! {
            if #[cfg(fusion)] {
                mod fusion;
                pub use fusion::*;
            } else {
                mod iour;
                pub use iour::*;
            }
        }
    } else {
        mod fallback;
        pub use fallback::*;
    }
}

/// Trait to get the selected buffer of an io operation.
/// 
/// 接口，用于读取某个IO操作关联的缓冲区。
pub trait TakeBuffer {
    /// Selected buffer type. It keeps the reference to the buffer pool and
    /// returns the buffer back on drop.
    /// 
    /// 缓冲区类型，其中包含对所属缓冲池的引用。
    type Buffer<'a>;

    /// Buffer pool type.
    /// 
    /// 缓冲池类型。
    type BufferPool;

    /// Take the selected buffer with `buffer_pool`, io `result` and `flags`, if
    /// io operation is success.
    /// 
    /// 传入含有缓冲区的编号的flags，含有缓冲区长度或IO操作结果的的result。
    fn take_buffer(
        self,
        buffer_pool: &Self::BufferPool,
        result: std::io::Result<usize>,
        flags: u32,
    ) -> std::io::Result<Self::Buffer<'_>>;
}
