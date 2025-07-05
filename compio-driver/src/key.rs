use std::{io, marker::PhantomData, mem::MaybeUninit, pin::Pin, task::Waker};

use compio_buf::BufResult;

use crate::{OpCode, Overlapped, PushEntry, RawFd};

/// An operation with other needed information. It should be allocated on the
/// heap. The pointer to this struct is used as `user_data`, and on Windows, it
/// is used as the pointer to `OVERLAPPED`.
///
/// 异步操作信息块：
/// - 携带了操作所需的所有信息。
/// - 在堆上进行分配，堆地址用作它的user_data值。
/// - 在windows中，它用作指向`OVERLAPPED`的指针。
///
/// `*const RawOp<dyn OpCode>` can be obtained from any `Key<T: OpCode>` by
/// first casting `Key::user_data` to `*const RawOp<()>`, then upcasted with
/// `upcast_fn`. It is done in [`Key::as_op_pin`].
///
/// 转换：
/// - `*const RawOp<dyn OpCode>`可通过转换任意的`Key<T:
///   OpCode>`进行获得，然后在通过`upcast_fn`上转。
/// - 方法：[`Key::as_op_pin`]
#[repr(C)]
pub(crate) struct RawOp<T: ?Sized> {
    header: Overlapped,
    /// The cancelled flag and the result here are manual reference counting.
    /// The driver holds the strong ref until it completes; the runtime
    /// holds the strong ref until the future is dropped.
    /// 可以看做一个是否正在被强引用的标记：
    /// - 驱动器持有强引用，直到任务完成。
    /// - 运行时持有强引用，直到future被遗弃。
    cancelled: bool,
    /// The metadata in `*mut RawOp<dyn OpCode>`
    /// 本类型的指针元数据
    /// - 对于SST类型的指针都为瘦指针，指针元数据为单元值，不占空间。
    /// - 对于DST类型的指针都为胖指针，指针元数据不是单元值，长度为一个字长。
    /// - 对于复合类型DST，其指针的元数据为其最后一个DST字段的元数据。
    /// - `RawOp`为复合DST，最后一个字段通常是一个操作码特质对象`<dyn OpCode>`，
    ///   因此此元数据即为特质对象的虚表地址，内容包含数据尺寸、 对齐值、
    ///   drop_in_place地址、方法表。
    metadata: usize,
    result: PushEntry<Option<Waker>, io::Result<usize>>,
    flags: u32,
    /// 操作码，通常使用动态尺寸类型dyn OpCode
    op: T,
}

/// 指针联合体
/// - ptr：指向RawOp的可变胖指针。最后一个字段DST类型为dyn OpCode。
/// - components：模拟特质对象*const dyn T指针。包含数据指针和元数据指针。
///
/// 这里的用法：
/// - 通过存储字段ptr，然后读取字段components，实现内存类型的转换，
///   类似transmute。
/// - 例子：ptr存入RawOp的指针，然后读取components，
///   实现了将RawOp的胖指针转为自定义类型OpCodePtrComponents。
#[repr(C)]
union OpCodePtrRepr {
    ptr: *mut RawOp<dyn OpCode>,
    components: OpCodePtrComponents,
}

/// 模拟特质对象指针。包含数据指针和元数据指针。
#[repr(C)]
#[derive(Clone, Copy)]
struct OpCodePtrComponents {
    data_pointer: *mut (),
    metadata: usize,
}

/// 取异步操作信息块对应的DST指针元数据
/// - 新建一个野的异步操作信息块RawOp<dyn OpCode>实例，取其DST胖指针。
/// - 将DST胖指针转为自定义类型OpCodePtrComponents，并返回元数据。
///
/// 问题：这里为什么取了一个野对象的元数据。
/// - 某个具体类型所有实例和某个特质构成的特质对象，都共享内存中同一份虚表。
///   因此对象指针的元数据也相同的，都是同一个虚表的地址值。
fn opcode_metadata<T: OpCode + 'static>() -> usize {
    let mut op = MaybeUninit::<RawOp<T>>::uninit();
    // SAFETY: same as `core::ptr::metadata`.
    unsafe {
        OpCodePtrRepr {
            ptr: op.as_mut_ptr(),
        }
        .components
        .metadata
    }
}

/// 利用RawOp的地址和dyn OpCode的元数据创建RawOp的胖指针。
const unsafe fn opcode_dyn_mut(ptr: *mut (), metadata: usize) -> *mut RawOp<dyn OpCode> {
    OpCodePtrRepr {
        components: OpCodePtrComponents {
            data_pointer: ptr,
            metadata,
        },
    }
    .ptr
}

/// A typed wrapper for key of Ops submitted into driver. It doesn't free the
/// inner on dropping. Instead, the memory is managed by the proactor. The inner
/// is only freed when:
///
/// 1. The op is completed and the future asks the result. `into_inner` will be
///    called by the proactor.
/// 2. The op is completed and the future cancels it. `into_box` will be called
///    by the proactor.
///
///
/// 异步操作信息块句柄，不具备回收信息块的功能，信息块由proactor负责管理。
/// - 异步操作的user_data：取信息块的内存地址数值。
/// - 异步操作的信息内容RawOp：存储在user_data所代表的地址中。
///
/// 异步操作信息块释放时机：
/// - op完成，future请求其结果。 proactor会调用`into_inner`。
/// - op完成，future取消了它。 proactor会调用`into_box`。
#[derive(PartialEq, Eq, Hash)]
pub struct Key<T: ?Sized> {
    user_data: *mut (),
    _p: PhantomData<Box<RawOp<T>>>,
}

impl<T: ?Sized> Unpin for Key<T> {}

impl<T: OpCode + 'static> Key<T> {
    /// Create [`RawOp`] and get the [`Key`] to it.
    pub(crate) fn new(driver: RawFd, op: T) -> Self {
        let header = Overlapped::new(driver);
        let raw_op = Box::new(RawOp {
            header,
            cancelled: false,
            metadata: opcode_metadata::<T>(),
            result: PushEntry::Pending(None),
            flags: 0,
            op,
        });
        unsafe { Self::new_unchecked(Box::into_raw(raw_op) as _) }
    }
}

impl<T: ?Sized> Key<T> {
    /// Create a new `Key` with the given user data.
    ///
    /// 使用给定的内存地址(user_data)，创建异步操作信息块的句柄。
    ///
    /// # Safety
    ///
    /// Caller needs to ensure that `T` does correspond to `user_data` in driver
    /// this `Key` is created with. In most cases, it is enough to let `T` be
    /// `dyn OpCode`.
    ///
    /// # 安全性
    /// 调用者必须确保`T`与期初创建`user_data`时对应的操作类型相同。
    /// 大多数情况下，把T的类型设为`dyn OpCode`就行了，即一个操作特质对象。
    pub unsafe fn new_unchecked(user_data: usize) -> Self {
        Self {
            user_data: user_data as _,
            _p: PhantomData,
        }
    }

    /// Get the unique user-defined data.
    ///
    /// 获取任务对应的user_data，实际使用异步操作内存块的地址。
    pub fn user_data(&self) -> usize {
        self.user_data as _
    }

    /// 从内存地址，获取异步操作信息块的只读引用。
    fn as_opaque(&self) -> &RawOp<()> {
        // SAFETY: user_data is unique and RawOp is repr(C).
        unsafe { &*(self.user_data as *const RawOp<()>) }
    }

    /// 从内存地址，获取异步操作信息块的可变引用。
    fn as_opaque_mut(&mut self) -> &mut RawOp<()> {
        // SAFETY: see `as_opaque`.
        unsafe { &mut *(self.user_data as *mut RawOp<()>) }
    }

    fn as_dyn_mut_ptr(&mut self) -> *mut RawOp<dyn OpCode> {
        let user_data = self.user_data;
        let this = self.as_opaque_mut();
        // SAFETY: metadata from `Key::new`.
        unsafe { opcode_dyn_mut(user_data, this.metadata) }
    }

    /// A pointer to OVERLAPPED.
    #[cfg(windows)]
    pub(crate) fn as_mut_ptr(&mut self) -> *mut Overlapped {
        &mut self.as_opaque_mut().header
    }

    /// Cancel the op, decrease the ref count. The return value indicates if the
    /// op is completed. If so, the op should be dropped because it is
    /// useless.
    ///
    /// 设置操作取消，并判断是否已经操作完毕了。
    pub(crate) fn set_cancelled(&mut self) -> bool {
        self.as_opaque_mut().cancelled = true;
        self.has_result()
    }

    /// Complete the op, decrease the ref count. Wake the future if a waker is
    /// set. The return value indicates if the op is cancelled. If so, the
    /// op should be dropped because it is useless.
    ///
    /// 操作完结
    /// - 设置提供的操作结果给操作信息块中的op。
    /// - 如果操作状态为Pending，则取出操作块中记录的唤醒器，执行唤醒。
    /// - 最终返回任务的取消状态。
    pub(crate) fn set_result(&mut self, res: io::Result<usize>) -> bool {
        let this = unsafe { &mut *self.as_dyn_mut_ptr() };
        #[cfg(io_uring)]
        if let Ok(res) = res {
            unsafe {
                Pin::new_unchecked(&mut this.op).set_result(res);
            }
        }
        if let PushEntry::Pending(Some(w)) =
            std::mem::replace(&mut this.result, PushEntry::Ready(res))
        {
            w.wake();
        }
        this.cancelled
    }

    pub(crate) fn set_flags(&mut self, flags: u32) {
        self.as_opaque_mut().flags = flags;
    }

    pub(crate) fn flags(&self) -> u32 {
        self.as_opaque().flags
    }

    /// Whether the op is completed.
    ///
    /// 判定操作是否已经成功完成。
    pub(crate) fn has_result(&self) -> bool {
        self.as_opaque().result.is_ready()
    }

    /// Set waker of the current future.
    ///
    /// 将当前future的唤醒器设给这个操作。
    pub(crate) fn set_waker(&mut self, waker: Waker) {
        if let PushEntry::Pending(w) = &mut self.as_opaque_mut().result {
            *w = Some(waker)
        }
    }

    /// Get the inner [`RawOp`]. It is usually used to drop the inner
    /// immediately, without knowing about the inner `T`.
    ///
    /// 获取[`RawOp`]的所有权，常用于立即销毁操作信息块，在T未知的情况下。
    ///
    /// # Safety
    ///
    /// Call it only when the op is cancelled and completed, which is the case
    /// when the ref count becomes zero. See doc of [`Key::set_cancelled`]
    /// and [`Key::set_result`].
    ///
    /// # 安全性
    ///
    /// 只有在操作被取消或已完成时，才能调用，此时强引用数变为0。
    pub(crate) unsafe fn into_box(mut self) -> Box<RawOp<dyn OpCode>> {
        Box::from_raw(self.as_dyn_mut_ptr())
    }
}

impl<T> Key<T> {
    /// Get the inner result if it is completed.
    ///
    /// 如果操作已经完成了，获取它的结果。
    ///
    /// # Safety
    ///
    /// Call it only when the op is completed, otherwise it is UB.
    ///
    /// # 安全性
    ///
    /// 只有在任务已完成的情况下才能调用此方法，否则会导致UB。
    pub(crate) unsafe fn into_inner(self) -> BufResult<usize, T> {
        let op = unsafe { Box::from_raw(self.user_data as *mut RawOp<T>) };
        BufResult(op.result.take_ready().unwrap_unchecked(), op.op)
    }
}

impl<T: OpCode + ?Sized> Key<T> {
    /// Pin the inner op.
    ///
    /// 获取并定住内部的异步操作特质对象。
    pub(crate) fn as_op_pin(&mut self) -> Pin<&mut dyn OpCode> {
        // SAFETY: the inner won't be moved.
        unsafe {
            let this = &mut *self.as_dyn_mut_ptr();
            Pin::new_unchecked(&mut this.op)
        }
    }

    /// Call [`OpCode::operate`] and assume that it is not an overlapped op,
    /// which means it never returns [`Poll::Pending`].
    ///
    /// [`Poll::Pending`]: std::task::Poll::Pending
    #[cfg(windows)]
    pub(crate) fn operate_blocking(&mut self) -> io::Result<usize> {
        use std::task::Poll;

        let optr = self.as_mut_ptr();
        let op = self.as_op_pin();
        let res = unsafe { op.operate(optr.cast()) };
        match res {
            Poll::Pending => unreachable!("this operation is not overlapped"),
            Poll::Ready(res) => res,
        }
    }
}

impl<T: ?Sized> std::fmt::Debug for Key<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Key({})", self.user_data())
    }
}
