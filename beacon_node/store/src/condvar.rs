use parking_lot::{Condvar, Mutex, RwLockWriteGuard};

/// `Condvar` supporting an `RwLock` guard.
///
/// Based on https://github.com/Amanieu/parking_lot/issues/165
pub struct CondvarAny {
    c: Condvar,
    m: Mutex<()>,
}

impl CondvarAny {
    pub fn new() -> Self {
        Self {
            c: Condvar::new(),
            m: Mutex::new(()),
        }
    }

    pub fn wait<T>(&self, g: &mut RwLockWriteGuard<'_, T>) {
        let guard = self.m.lock();
        RwLockWriteGuard::unlocked(g, || {
            // Move the guard in so it gets unlocked before we re-lock g
            let mut guard = guard;
            self.c.wait(&mut guard);
        });
    }

    pub fn notify_one(&self) -> bool {
        self.c.notify_one()
    }
}
