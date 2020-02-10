use std::sync::atomic;

// todo: this is the generic impl, specialize for x64

#[inline]
#[allow(dead_code)]
pub fn thread_fence() {
    atomic::fence(atomic::Ordering::AcqRel)
}

#[inline]
#[allow(dead_code)]
pub fn fence() {
    atomic::fence(atomic::Ordering::SeqCst)
}

#[inline]
#[allow(dead_code)]
pub fn acquire() {
    atomic::fence(atomic::Ordering::Acquire)
}

#[inline]
#[allow(dead_code)]
pub fn release() {
    atomic::fence(atomic::Ordering::Release)
}

#[inline]
#[allow(dead_code)]
pub fn cpu_pause() {
    ::std::thread::yield_now();
}
