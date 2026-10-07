use crate::common::{OnceLockResult, TreeNodeExt};
use crate::metrics::snapshot::{metrics_equal, snapshot_metrics};
use crate::worker::task_data::TaskDataMetrics;
use crate::{
    DistributedTaskContext, TaskKey, TaskMetrics, TaskMetricsUpdate, WorkerToCoordinatorMsg,
};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::common::{HashMap, HashSet, Result};
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::{ExecutionPlan, RecordBatchStream};
use futures::stream::BoxStream;
use futures::{Stream, StreamExt};
use std::pin::Pin;
use std::sync::{Arc, Mutex, OnceLock};
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::sync::Notify;
use tokio::time::{Instant, MissedTickBehavior, interval_at};
use uuid::Uuid;

/// One demand-driven reporter per worker/query, carried by the first task's control stream.
#[derive(Default)]
pub(super) struct WorkerMetrics {
    queries: Mutex<HashMap<Uuid, Arc<QueryMetrics>>>,
}

#[derive(Default)]
struct QueryMetrics {
    tasks: Mutex<HashMap<TaskKey, ReportedTask>>,
    changed: Notify,
}

struct ReportedTask {
    source: Option<Arc<TaskMetricsSource>>,
    previous: Option<TaskMetrics>,
}

/// Removing a task at coordinator EOS also wakes the carrier so it can terminate promptly.
pub(super) struct MetricsRegistration {
    reporters: Arc<WorkerMetrics>,
    key: TaskKey,
}

impl Drop for MetricsRegistration {
    fn drop(&mut self) {
        let mut queries = self.reporters.queries.lock().unwrap();
        if let Some(query) = queries.get(&self.key.query_id) {
            let empty = {
                let mut tasks = query.tasks.lock().unwrap();
                tasks.remove(&self.key);
                tasks.is_empty()
            };
            query.changed.notify_one();
            if empty {
                queries.remove(&self.key.query_id);
            }
        }
    }
}

impl WorkerMetrics {
    pub(super) fn register(
        self: &Arc<Self>,
        key: TaskKey,
        source: Arc<TaskMetricsSource>,
        interval: Duration,
    ) -> (
        MetricsRegistration,
        BoxStream<'static, WorkerToCoordinatorMsg>,
    ) {
        let mut queries = self.queries.lock().unwrap();
        let carrier = !queries.contains_key(&key.query_id);
        let query = Arc::clone(queries.entry(key.query_id).or_default());
        query.tasks.lock().unwrap().insert(
            key,
            ReportedTask {
                source: Some(source),
                previous: None,
            },
        );
        let registration = MetricsRegistration {
            reporters: Arc::clone(self),
            key,
        };
        if !carrier {
            return (registration, futures::stream::empty().boxed());
        }
        let mut ticker = interval_at(Instant::now() + interval, interval);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
        let stream = futures::stream::unfold((query, ticker), |(query, mut ticker)| async move {
            loop {
                if query.tasks.lock().unwrap().is_empty() {
                    return None;
                }
                tokio::select! {
                    _ = query.changed.notified() => continue,
                    _ = ticker.tick() => {},
                }
                let updates = query.sample();
                if !updates.is_empty() {
                    return Some((
                        WorkerToCoordinatorMsg::MetricsUpdate(updates),
                        (query, ticker),
                    ));
                }
            }
        })
        .boxed();
        (registration, stream)
    }
}

impl QueryMetrics {
    fn sample(&self) -> Vec<TaskMetricsUpdate> {
        let mut tasks = self.tasks.lock().unwrap();
        tasks
            .iter_mut()
            .filter_map(|(key, task)| {
                let source = task.source.as_ref()?;
                let (current, completed) = source.snapshot()?;
                let changed = !task.previous.as_ref().is_some_and(|previous| {
                    previous.pre_order_plan_metrics.len() == current.pre_order_plan_metrics.len()
                        && previous
                            .pre_order_plan_metrics
                            .iter()
                            .zip(&current.pre_order_plan_metrics)
                            .all(|(a, b)| metrics_equal(a, b))
                        && metrics_equal(&previous.task_metrics, &current.task_metrics)
                });
                if completed {
                    task.source = None;
                }
                task.previous = (!completed).then(|| current.clone());
                changed.then_some(TaskMetricsUpdate {
                    task_key: *key,
                    metrics: current,
                })
            })
            .collect()
    }
}

#[derive(Debug)]
pub(super) struct TaskMetricsSource {
    plan: Arc<OnceLockResult<Arc<dyn ExecutionPlan>>>,
    metrics: Arc<TaskDataMetrics>,
    context: DistributedTaskContext,
    finished_partitions: Mutex<HashSet<usize>>,
    completed: OnceLock<TaskMetrics>,
}

impl TaskMetricsSource {
    pub(super) fn new(
        plan: Arc<OnceLockResult<Arc<dyn ExecutionPlan>>>,
        metrics: Arc<TaskDataMetrics>,
        context: DistributedTaskContext,
    ) -> Self {
        Self {
            plan,
            metrics,
            context,
            finished_partitions: Mutex::new(HashSet::new()),
            completed: OnceLock::new(),
        }
    }

    fn snapshot(&self) -> Option<(TaskMetrics, bool)> {
        if let Some(completed) = self.completed.get() {
            return Some((completed.clone(), true));
        }
        let plan = self.plan.get()?.as_ref().ok()?;
        Some((
            collect_task_metrics(plan, self.context, &self.metrics),
            false,
        ))
    }

    fn partition_finished(&self, partition: usize) {
        let Some(Ok(plan)) = self.plan.get() else {
            return;
        };
        let mut finished = self.finished_partitions.lock().unwrap();
        finished.insert(partition);
        if finished.len() == plan.properties().partitioning.partition_count() {
            self.completed.get_or_init(|| {
                self.metrics.mark_execution_finished();
                collect_task_metrics(plan, self.context, &self.metrics)
            });
        }
    }

    pub(super) fn track(
        self: &Arc<Self>,
        inner: SendableRecordBatchStream,
        partition: usize,
    ) -> SendableRecordBatchStream {
        Box::pin(MetricsStream {
            schema: inner.schema(),
            inner: Some(inner),
            source: Arc::clone(self),
            partition,
        })
    }
}

pub(super) fn collect_task_metrics(
    plan: &Arc<dyn ExecutionPlan>,
    context: DistributedTaskContext,
    metrics: &TaskDataMetrics,
) -> TaskMetrics {
    let mut pre_order_plan_metrics = vec![];
    let _ = plan.apply_with_dt_ctx(context, |node, _| {
        pre_order_plan_metrics.push(snapshot_metrics(&node.metrics().unwrap_or_default()));
        Ok(TreeNodeRecursion::Continue)
    });
    TaskMetrics {
        pre_order_plan_metrics,
        task_metrics: snapshot_metrics(&metrics.to_metrics_set()),
    }
}

/// Drop the inner stream before reading metrics finalized in stream destructors.
struct MetricsStream {
    schema: SchemaRef,
    inner: Option<SendableRecordBatchStream>,
    source: Arc<TaskMetricsSource>,
    partition: usize,
}

impl MetricsStream {
    fn finish(&mut self) {
        if let Some(inner) = self.inner.take() {
            drop(inner);
            self.source.partition_finished(self.partition);
        }
    }
}

impl Stream for MetricsStream {
    type Item = Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let Some(inner) = self.inner.as_mut() else {
            return Poll::Ready(None);
        };
        let result = inner.as_mut().poll_next(cx);
        if matches!(result, Poll::Ready(None | Some(Err(_)))) {
            self.finish();
        }
        result
    }
}

impl RecordBatchStream for MetricsStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

impl Drop for MetricsStream {
    fn drop(&mut self) {
        self.finish();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::now_ns;
    use crate::config_extension_ext::get_config_extension_propagation_headers;
    use crate::test_utils::mock_exec::MockExec;
    use crate::worker::task_data::{PLAN_EXECUTED_AT_METRIC, PLAN_FINISHED_AT_METRIC};
    use crate::{
        DistributedConfig, DistributedExt, ExecuteTaskRequest, MaybeEncoded, ProducerHead,
        SetPlanRequest, Worker,
    };
    use datafusion::arrow::datatypes::Schema;
    use datafusion::execution::TaskContext;
    use datafusion::physical_plan::empty::EmptyExec;
    use datafusion::prelude::SessionConfig;
    use futures::TryStreamExt;
    use tokio::time::timeout;
    use tokio_stream::wrappers::UnboundedReceiverStream;
    use url::Url;

    #[tokio::test]
    async fn one_carrier_batches_many_tasks_and_coalesces_updates() {
        let reporters = Arc::new(WorkerMetrics::default());
        let source = source();
        let mut registrations = vec![];
        let mut carrier = None;
        for task_number in 0..1000 {
            let (registration, mut stream) = reporters.register(
                TaskKey {
                    query_id: Uuid::from_u128(1),
                    stage_id: task_number % 4,
                    task_number,
                },
                Arc::clone(&source),
                Duration::from_millis(1),
            );
            registrations.push(registration);
            if task_number == 0 {
                carrier = Some(stream);
            } else {
                assert!(stream.next().await.is_none());
            }
        }
        let mut carrier = carrier.unwrap();
        let first = batch(&mut carrier).await;
        assert_eq!(first.len(), 1000);
        assert!(
            first
                .iter()
                .all(|task| !task.metrics.pre_order_plan_metrics.is_empty())
        );
        source.metrics.mark_execution_started_once();
        let second = batch(&mut carrier).await;
        assert_eq!(second.len(), 1000);
        assert_eq!(
            first[0]
                .metrics
                .task_metrics
                .sum(|metric| metric.value().name() == PLAN_EXECUTED_AT_METRIC)
                .unwrap()
                .as_usize(),
            0
        );
        assert!(
            second[0]
                .metrics
                .task_metrics
                .sum(|metric| metric.value().name() == PLAN_EXECUTED_AT_METRIC)
                .unwrap()
                .as_usize()
                > 0
        );
        assert!(
            timeout(Duration::from_millis(10), carrier.next())
                .await
                .is_err()
        );
        // Closing the carrier's own task must not end reporting for the other tasks.
        drop(registrations.remove(0));
        assert!(
            timeout(Duration::from_millis(10), carrier.next())
                .await
                .is_err()
        );
        drop(registrations);
        assert!(
            timeout(Duration::from_secs(1), carrier.next())
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn completion_waits_for_all_partitions_and_survives_between_ticks() {
        let reporters = Arc::new(WorkerMetrics::default());
        let source = source();
        let key = TaskKey {
            query_id: Uuid::from_u128(1),
            stage_id: 0,
            task_number: 0,
        };
        let (_registration, mut carrier) =
            reporters.register(key, Arc::clone(&source), Duration::from_millis(1));
        let plan = source.plan.get().unwrap().as_ref().unwrap();
        let ctx = Arc::new(TaskContext::default());
        let first = source.track(plan.execute(0, Arc::clone(&ctx)).unwrap(), 0);
        first.try_collect::<Vec<_>>().await.unwrap();
        assert_eq!(
            batch(&mut carrier).await[0]
                .metrics
                .task_metrics
                .sum(|metric| metric.value().name() == PLAN_FINISHED_AT_METRIC)
                .unwrap()
                .as_usize(),
            0
        );
        // The second partition is requested later and then dropped without polling.
        drop(source.track(plan.execute(1, ctx).unwrap(), 1));
        let completed = batch(&mut carrier).await;
        assert!(
            completed[0]
                .metrics
                .task_metrics
                .sum(|metric| metric.value().name() == PLAN_FINISHED_AT_METRIC)
                .unwrap()
                .as_usize()
                > 0
        );
        source.metrics.mark_execution_started_once();
        assert!(
            timeout(Duration::from_millis(10), carrier.next())
                .await
                .is_err()
        );
    }

    #[test_case::test_case(0, true, true; "terminal_only")]
    #[test_case::test_case(1, false, true; "collection_disabled")]
    #[test_case::test_case(1, true, false; "unexecuted")]
    #[tokio::test]
    async fn disabled_or_unexecuted_tasks_close_without_live_reports(
        interval: u64,
        collect: bool,
        execute: bool,
    ) {
        let worker = Worker::default();
        let config = SessionConfig::new().with_distributed_option_extension(DistributedConfig {
            collect_metrics: collect,
            metrics_reporting_interval_ms: interval,
            ..Default::default()
        });
        let key = TaskKey {
            query_id: Uuid::from_u128(1),
            stage_id: 0,
            task_number: 0,
        };
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let mut response = worker
            .coordinator_channel(
                get_config_extension_propagation_headers(&config).unwrap(),
                SetPlanRequest {
                    task_key: key,
                    task_count: 1,
                    plan: MaybeEncoded::Decoded(Arc::new(EmptyExec::new(
                        Arc::new(Schema::empty()),
                    ))),
                    dynamic_filter_remote_producer_ids: vec![],
                    work_unit_feed_declarations: vec![],
                    target_worker_url: Url::parse("http://worker").unwrap(),
                    query_start_time_ns: now_ns(),
                },
                UnboundedReceiverStream::new(rx).boxed(),
            )
            .await
            .unwrap()
            .stream;
        if execute {
            let (mut streams, _) = worker
                .execute_task(ExecuteTaskRequest {
                    task_key: key,
                    target_partition_start: 0,
                    target_partition_end: 1,
                    producer_head: ProducerHead::None,
                })
                .await
                .unwrap();
            streams
                .pop()
                .unwrap()
                .try_collect::<Vec<_>>()
                .await
                .unwrap();
        }
        assert!(matches!(
            response.try_next().await.unwrap(),
            Some(WorkerToCoordinatorMsg::LoadInfoEos)
        ));
        assert!(
            timeout(Duration::from_millis(10), response.next())
                .await
                .is_err()
        );
        drop(tx);
        let reports = timeout(Duration::from_secs(1), response.try_collect::<Vec<_>>())
            .await
            .unwrap()
            .unwrap();
        assert!(
            !reports
                .iter()
                .any(|report| matches!(report, WorkerToCoordinatorMsg::MetricsUpdate(_)))
        );
        assert_eq!(
            reports
                .iter()
                .any(|report| matches!(report, WorkerToCoordinatorMsg::TaskMetrics(_))),
            execute && collect
        );
        assert_eq!(worker.tasks_running().await, 0);
    }

    fn source() -> Arc<TaskMetricsSource> {
        let plan: Arc<dyn ExecutionPlan> = Arc::new(
            MockExec::new_partitioned(vec![vec![], vec![]], Arc::new(Schema::empty()))
                .with_use_task(false),
        );
        Arc::new(TaskMetricsSource::new(
            Arc::new(OnceLock::from(Ok(plan))),
            Arc::new(TaskDataMetrics::new(now_ns())),
            DistributedTaskContext {
                task_index: 0,
                task_count: 1,
            },
        ))
    }

    async fn batch(
        stream: &mut BoxStream<'static, WorkerToCoordinatorMsg>,
    ) -> Vec<TaskMetricsUpdate> {
        match timeout(Duration::from_secs(1), stream.next())
            .await
            .unwrap()
            .unwrap()
        {
            WorkerToCoordinatorMsg::MetricsUpdate(batch) => batch,
            _ => panic!("expected metrics batch"),
        }
    }
}
