use futures::Future;
use futures_intrusive::sync::ManualResetEvent;
use std::sync::atomic::{AtomicUsize, Ordering};

pub struct WaitGroup {
    pending: AtomicUsize,
    done: ManualResetEvent,
}

impl WaitGroup {
    pub fn new(count: usize) -> Self {
        WaitGroup {
            pending: AtomicUsize::new(count),
            done: ManualResetEvent::new(false),
        }
    }

    pub fn done(&self) {
        if self.pending.fetch_sub(1, Ordering::SeqCst) == 1 {
            self.done.set();
        }
    }

    pub fn wait<'a>(&'a self) -> impl Future<Output = ()> + 'a {
        self.done.wait()
    }
}
