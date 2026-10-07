use crate::{TaskKey, TaskMetrics, TaskMetricsUpdate};
use datafusion::common::{HashMap, HashSet, Result, exec_err};
use futures::StreamExt;
use futures::stream::BoxStream;
use tokio::sync::watch;
use uuid::Uuid;

#[derive(Debug, Default)]
struct MetricsState {
    metrics: HashMap<TaskKey, TaskMetrics>,
    final_tasks: HashSet<TaskKey>,
    closed_tasks: HashSet<TaskKey>,
}

/// Latest values and report status share one lock. Notifications retain no history.
#[derive(Debug)]
pub(crate) struct MetricsStore {
    tx: watch::Sender<MetricsState>,
}

impl MetricsStore {
    pub(crate) fn new() -> Self {
        let (tx, _) = watch::channel(MetricsState::default());
        Self { tx }
    }

    pub(crate) fn insert(&self, key: TaskKey, metrics: TaskMetrics) {
        self.tx.send_modify(|state| {
            state.final_tasks.insert(key);
            state.closed_tasks.insert(key);
            state.metrics.insert(key, metrics);
        });
    }

    pub(crate) fn close(&self, key: TaskKey) {
        self.tx
            .send_if_modified(|state| state.closed_tasks.insert(key));
    }

    pub(crate) fn update(&self, query_id: Uuid, updates: Vec<TaskMetricsUpdate>) -> Result<()> {
        if updates
            .iter()
            .any(|update| update.task_key.query_id != query_id)
        {
            return exec_err!("Live metrics batch contains a task from another query");
        }
        self.tx.send_if_modified(|state| {
            let mut changed = false;
            for update in updates {
                // A batch on another task's channel can arrive after this task's final report.
                if !state.final_tasks.contains(&update.task_key) {
                    state.metrics.insert(update.task_key, update.metrics);
                    changed = true;
                }
            }
            changed
        });
        Ok(())
    }

    pub(crate) fn snapshot(&self, expected: &[TaskKey]) -> (HashMap<TaskKey, TaskMetrics>, bool) {
        let state = self.tx.borrow();
        let complete = expected.iter().all(|key| state.final_tasks.contains(key));
        (state.metrics.clone(), complete)
    }

    pub(crate) fn notify(&self) {
        self.tx.send_modify(|_| {});
    }

    pub(crate) fn updates(&self) -> BoxStream<'static, ()> {
        let mut rx = self.tx.subscribe();
        // Include coordinator-local progress when subscribing after preparation, even if
        // no worker has reported yet. An early snapshot may still be unavailable.
        rx.mark_changed();
        futures::stream::unfold(rx, |mut rx| async move {
            rx.changed().await.ok()?;
            Some(((), rx))
        })
        .boxed()
    }

    pub(crate) async fn wait_for(&self, expected: &[TaskKey]) -> HashMap<TaskKey, TaskMetrics> {
        let mut rx = self.tx.subscribe();
        let _ = rx
            .wait_for(|state| expected.iter().all(|key| state.closed_tasks.contains(key)))
            .await;
        let state = rx.borrow();
        // Preserve terminal-only behavior: live values never substitute for a final report.
        state
            .closed_tasks
            .iter()
            .map(|key| {
                let metrics = if state.final_tasks.contains(key) {
                    state.metrics.get(key).cloned().unwrap_or_default()
                } else {
                    TaskMetrics::default()
                };
                (*key, metrics)
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::physical_plan::metrics::{Gauge, Metric, MetricValue, MetricsSet};
    use futures::FutureExt;
    use std::sync::Arc;

    #[tokio::test]
    async fn notifications_coalesce_and_final_reports_win() {
        let store = MetricsStore::new();
        let key = key();
        let mut updates = store.updates();
        assert_eq!(updates.next().await, Some(()));
        assert!(updates.next().now_or_never().is_none());
        for value in [100, 3] {
            store
                .update(
                    key.query_id,
                    vec![TaskMetricsUpdate {
                        task_key: key,
                        metrics: metrics(value),
                    }],
                )
                .unwrap();
        }
        assert_eq!(updates.next().await, Some(()));
        assert!(updates.next().now_or_never().is_none());
        assert_eq!(value(&store.snapshot(&[key]).0[&key]), 3);
        assert!(!store.snapshot(&[key]).1);
        assert!(store.wait_for(&[key]).now_or_never().is_none());

        store.insert(key, metrics(7));
        assert_eq!(updates.next().await, Some(()));
        store
            .update(
                key.query_id,
                vec![TaskMetricsUpdate {
                    task_key: key,
                    metrics: metrics(99),
                }],
            )
            .unwrap();
        assert!(updates.next().now_or_never().is_none());
        assert!(store.snapshot(&[key]).1);
        assert_eq!(value(&store.wait_for(&[key]).await[&key]), 7);
        assert_eq!(store.updates().next().await, Some(()));
        drop(store);
        assert_eq!(updates.next().await, None);
    }

    #[tokio::test]
    async fn closed_tasks_keep_live_values_only_in_snapshots() {
        let store = MetricsStore::new();
        let key = key();
        store
            .update(
                key.query_id,
                vec![TaskMetricsUpdate {
                    task_key: key,
                    metrics: metrics(7),
                }],
            )
            .unwrap();
        store.close(key);
        assert_eq!(value(&store.snapshot(&[key]).0[&key]), 7);
        assert!(!store.snapshot(&[key]).1);
        assert!(
            store.wait_for(&[key]).await[&key]
                .pre_order_plan_metrics
                .is_empty()
        );
    }

    #[test]
    fn another_query_is_rejected_without_applying_any_reports() {
        let store = MetricsStore::new();
        let key = key();
        assert!(
            store
                .update(
                    Uuid::nil(),
                    vec![TaskMetricsUpdate {
                        task_key: key,
                        metrics: metrics(7)
                    }]
                )
                .is_err()
        );
        assert!(store.snapshot(&[key]).0.is_empty());
    }

    fn key() -> TaskKey {
        TaskKey {
            query_id: Uuid::from_u128(1),
            stage_id: 2,
            task_number: 3,
        }
    }

    fn metrics(value: usize) -> TaskMetrics {
        let gauge = Gauge::new();
        gauge.set(value);
        let mut metrics = MetricsSet::new();
        metrics.push(Arc::new(Metric::new(
            MetricValue::CurrentMemoryUsage(gauge),
            None,
        )));
        TaskMetrics {
            pre_order_plan_metrics: vec![metrics],
            ..Default::default()
        }
    }

    fn value(metrics: &TaskMetrics) -> usize {
        metrics.pre_order_plan_metrics[0]
            .iter()
            .next()
            .unwrap()
            .value()
            .as_usize()
    }
}
