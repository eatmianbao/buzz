//! Per-key single-flight refresh with a minimum interval.
//!
//! A refresh for a key runs at once when none is in flight. Requests that
//! arrive while one is running or cooling down collapse into one trailing
//! rerun, so a key refreshes at most once per interval per process and the
//! last request is always covered by a refresh that started after it.

use std::future::Future;
use std::hash::Hash;
use std::sync::Arc;
use std::time::Duration;

use dashmap::mapref::entry::Entry;
use dashmap::DashMap;

/// Coalesces refresh requests per key. The map holds only keys with a
/// refresh in flight; the value is "another request arrived since it began".
pub struct CoalescedRefresh<K> {
    pending: DashMap<K, bool>,
    min_interval: Duration,
}

impl<K> CoalescedRefresh<K>
where
    K: Eq + Hash + Clone + Send + Sync + 'static,
{
    /// A coalescer that refreshes each key at most once per `min_interval`.
    pub fn new(min_interval: Duration) -> Self {
        Self {
            pending: DashMap::new(),
            min_interval,
        }
    }

    /// Request a refresh of `key`. Runs `refresh` on a spawned task now when
    /// `key` is idle; otherwise marks the in-flight refresh to run once more
    /// after its interval elapses.
    pub fn request<F, Fut>(self: &Arc<Self>, key: K, refresh: F)
    where
        F: Fn() -> Fut + Send + 'static,
        Fut: Future<Output = ()> + Send,
    {
        match self.pending.entry(key.clone()) {
            Entry::Occupied(mut rerun) => {
                *rerun.get_mut() = true;
                return;
            }
            Entry::Vacant(slot) => {
                slot.insert(false);
            }
        }
        let this = Arc::clone(self);
        tokio::spawn(async move {
            // Frees the key if the task panics or is cancelled, so a failed
            // refresh can't block the key forever.
            let mut release = Release {
                pending: &this.pending,
                key: Some(key.clone()),
            };
            loop {
                refresh().await;
                tokio::time::sleep(this.min_interval).await;
                if !this.rerun_or_release(&key) {
                    break;
                }
            }
            release.key = None;
        });
    }

    /// Clear the rerun flag and report it, or free the key when no request
    /// arrived. Atomic with [`Self::request`]'s entry check, so a request
    /// either sets the flag before this reads it or finds the key free.
    fn rerun_or_release(&self, key: &K) -> bool {
        match self.pending.entry(key.clone()) {
            Entry::Occupied(mut entry) if *entry.get() => {
                *entry.get_mut() = false;
                true
            }
            Entry::Occupied(entry) => {
                entry.remove();
                false
            }
            Entry::Vacant(_) => false,
        }
    }
}

struct Release<'a, K: Eq + Hash> {
    pending: &'a DashMap<K, bool>,
    key: Option<K>,
}

impl<K: Eq + Hash> Drop for Release<'_, K> {
    fn drop(&mut self) {
        if let Some(key) = self.key.take() {
            self.pending.remove(&key);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    const INTERVAL: Duration = Duration::from_secs(1);

    fn counting(runs: &Arc<AtomicUsize>) -> impl Fn() -> std::future::Ready<()> + Send + 'static {
        let runs = Arc::clone(runs);
        move || {
            runs.fetch_add(1, Ordering::SeqCst);
            std::future::ready(())
        }
    }

    /// Let spawned tasks run up to their next timer.
    async fn settle() {
        for _ in 0..10 {
            tokio::task::yield_now().await;
        }
    }

    #[tokio::test(start_paused = true)]
    async fn burst_runs_once_now_and_once_trailing_then_frees_the_key() {
        let refresh = Arc::new(CoalescedRefresh::new(INTERVAL));
        let runs = Arc::new(AtomicUsize::new(0));

        for _ in 0..100 {
            refresh.request("root", counting(&runs));
        }
        settle().await;
        assert_eq!(runs.load(Ordering::SeqCst), 1, "first request runs at once");

        tokio::time::advance(INTERVAL).await;
        settle().await;
        assert_eq!(
            runs.load(Ordering::SeqCst),
            2,
            "the burst gets one trailing run"
        );

        tokio::time::advance(INTERVAL).await;
        settle().await;
        assert_eq!(runs.load(Ordering::SeqCst), 2, "no request, no rerun");
        assert!(refresh.pending.is_empty(), "idle key is released");

        refresh.request("root", counting(&runs));
        settle().await;
        assert_eq!(
            runs.load(Ordering::SeqCst),
            3,
            "a later request runs at once"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn keys_refresh_independently() {
        let refresh = Arc::new(CoalescedRefresh::new(INTERVAL));
        let runs = Arc::new(AtomicUsize::new(0));

        refresh.request("a", counting(&runs));
        refresh.request("b", counting(&runs));
        settle().await;
        assert_eq!(runs.load(Ordering::SeqCst), 2);
    }

    #[tokio::test(start_paused = true)]
    async fn panicking_refresh_releases_the_key() {
        let refresh = Arc::new(CoalescedRefresh::new(INTERVAL));
        refresh.request("root", || async { panic!("refresh failed") });
        settle().await;
        assert!(refresh.pending.is_empty(), "panic must not wedge the key");

        let runs = Arc::new(AtomicUsize::new(0));
        refresh.request("root", counting(&runs));
        settle().await;
        assert_eq!(runs.load(Ordering::SeqCst), 1);
    }
}
