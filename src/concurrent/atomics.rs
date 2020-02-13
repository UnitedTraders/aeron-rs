use std::sync::atomic;

// todo: this is the generic impl, specialize for x64

#[inline]
pub fn thread_fence() {
    atomic::fence(atomic::Ordering::AcqRel)
}

#[inline]
pub fn fence() {
    atomic::fence(atomic::Ordering::SeqCst)
}

#[inline]
pub fn acquire() {
    atomic::fence(atomic::Ordering::Acquire)
}

#[inline]
pub fn release() {
    atomic::fence(atomic::Ordering::Release)
}

#[inline]
pub fn cpu_pause() {
    ::std::thread::yield_now();
}
