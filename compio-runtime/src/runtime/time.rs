use std::{
    cmp::Reverse,
    collections::BinaryHeap,
    future::Future,
    pin::Pin,
    task::{Context, Poll, Waker},
    time::{Duration, Instant},
};

use slab::Slab;

use crate::runtime::Runtime;

/// 定时器状态，存在slab缓存中的。
pub(crate) enum FutureState {
    Active(Option<Waker>),
    Completed,
}

impl Default for FutureState {
    fn default() -> Self {
        Self::Active(None)
    }
}

/// 定时器条目，存在排序的二叉树中。
/// - 通过key与slab中的状态关联。
/// - 必须实现排序，因为要插入二叉树中按照到期时间增序排列。
#[derive(Debug)]
struct TimerEntry {
    key: usize,
    delay: Duration,
}

impl PartialEq for TimerEntry {
    fn eq(&self, other: &Self) -> bool {
        self.delay == other.delay
    }
}

impl Eq for TimerEntry {}

impl PartialOrd for TimerEntry {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for TimerEntry {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.delay.cmp(&other.delay)
    }
}

/// 定时器运行时。
pub struct TimerRuntime {
    time: Instant,
    tasks: Slab<FutureState>,
    wheel: BinaryHeap<Reverse<TimerEntry>>,
}

impl TimerRuntime {
    pub fn new() -> Self {
        Self {
            time: Instant::now(),
            tasks: Slab::default(),
            wheel: BinaryHeap::default(),
        }
    }

    /// 判定定时操作是否完成。取消和到期都算完成了。如果完成了返回true。
    ///
    /// 判定逻辑：
    /// - 从操作中心缓存中找到对应的操作。
    /// - 如果操作状态为Completed，则完成。
    /// - 如果没找到，则表明操作取消了，也算完成。
    pub fn is_completed(&self, key: usize) -> bool {
        self.tasks
            .get(key)
            .map(|state| matches!(state, FutureState::Completed))
            .unwrap_or_default()
    }

    /// 新建一个定时操作：
    /// - 如果想定时的时间已经过了，则定时失败，返回None。
    /// - 向定时中心插入一条状态为Active(None)的定时操作状态。
    /// - 向定时器二叉树中插入定时条目。
    ///
    /// 如何判定定时时间已经过了：
    /// - 运行时启动后流过的时间间隔，
    ///   大于想要插入的时间与运行时启动点的时间间隔。
    pub fn insert(&mut self, instant: Instant) -> Option<usize> {
        let delay = instant - self.time;
        if delay <= self.time.elapsed() {
            return None;
        }
        let key = self.tasks.insert(FutureState::Active(None));
        let entry = TimerEntry { key, delay };
        self.wheel.push(Reverse(entry));
        Some(key)
    }

    /// 更新定时器唤醒器。
    ///
    /// 将唤醒器更新到定时器状态中。
    pub fn update_waker(&mut self, key: usize, waker: Waker) {
        if let Some(w) = self.tasks.get_mut(key) {
            *w = FutureState::Active(Some(waker));
        }
    }

    /// 取消定时器。
    ///
    /// 直接将定时器状态移除。
    pub fn cancel(&mut self, key: usize) {
        self.tasks.remove(key);
    }

    /// 计算最近即将到期的定时器还有多久到期。用于确定下次定时器轮询时间。
    /// - 如果最近的定时器已经到期，则返回ZERO。(需要立即轮询)
    /// - 如果最近的定时器还未到期，则计算还有多久到期并返回。
    pub fn min_timeout(&self) -> Option<Duration> {
        self.wheel.peek().map(|entry| {
            let elapsed = self.time.elapsed();
            if entry.0.delay > elapsed {
                entry.0.delay - elapsed
            } else {
                Duration::ZERO
            }
        })
    }

    /// 轮询定时器，唤醒已经到期的定时器对应的任务。
    ///
    /// 遍历二叉树
    /// - 到期：如果定时时间小于当前时间，则说明到期了，从树中取出
    ///   - 修改定时器状态为Completed。
    ///   - 执行唤醒。
    /// - 未到期：如果定时时间依然大于当前时间，则将定时器插回二叉树中。
    pub fn wake(&mut self) {
        if self.wheel.is_empty() {
            return;
        }
        // 当前时间
        let elapsed = self.time.elapsed();
        while let Some(entry) = self.wheel.pop() {
            if entry.0.delay <= elapsed {
                if let Some(state) = self.tasks.get_mut(entry.0.key) {
                    let old_state = std::mem::replace(state, FutureState::Completed);
                    if let FutureState::Active(Some(waker)) = old_state {
                        waker.wake();
                    }
                }
            } else {
                self.wheel.push(entry);
                break;
            }
        }
    }
}

/// 定时操作对应的异步对象。
pub struct TimerFuture {
    key: usize,
}

impl TimerFuture {
    pub fn new(key: usize) -> Self {
        Self { key }
    }
}

impl Future for TimerFuture {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Runtime::with_current(|r| r.poll_timer(cx, self.key))
    }
}

impl Drop for TimerFuture {
    fn drop(&mut self) {
        Runtime::with_current(|r| r.cancel_timer(self.key));
    }
}

#[test]
fn timer_min_timeout() {
    let mut runtime = TimerRuntime::new();
    assert_eq!(runtime.min_timeout(), None);

    let now = Instant::now();
    runtime.insert(now + Duration::from_secs(1));
    runtime.insert(now + Duration::from_secs(10));
    let min_timeout = runtime.min_timeout().unwrap().as_secs_f32();

    assert!(min_timeout < 1.);
}
