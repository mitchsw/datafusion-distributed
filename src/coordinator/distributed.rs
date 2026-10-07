use crate::common::require_one_child;
use crate::coordinator::metrics_store::MetricsStore;
use crate::coordinator::prepare_dynamic_plan::prepare_dynamic_plan;
use crate::coordinator::prepare_static_plan::prepare_static_plan;
use crate::coordinator::query_coordinator::QueryCoordinator;
use crate::coordinator::store::{Store, task_keys_for_plan};
use crate::dynamic_filtering::{
    is_local_dynamic_filtering_enabled, sever_dynamic_filter_relationships_in_plan_for_display,
};
use crate::metrics::collect_plan_metrics;
use crate::metrics::snapshot::{metrics_equal, snapshot_metrics};
use crate::{DistributedConfig, TaskCompletedDynamicFilters, TaskKey, TaskMetrics};
use datafusion::common::internal_datafusion_err;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::common::{HashMap, Result, exec_err};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr_common::metrics::MetricsSet;
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion::physical_plan::stream::RecordBatchReceiverStreamBuilder;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use futures::StreamExt;
use futures::stream::BoxStream;
use std::fmt::Formatter;
use std::sync::{Arc, OnceLock};
use std::time::Duration;
use tokio::time::{Instant, MissedTickBehavior, interval_at};

/// [ExecutionPlan] that executes the inner plan in distributed mode.
/// Before executing it, two modifications are lazily performed on the plan:
/// 1. Assigns worker URLs to all the stages. Unless explicitly set in
///    [crate::RouteTasksHandler], a random set of URLs are sampled from the
///    channel resolver and assigned to each task in each stage.
/// 2. Encodes all the plans in protobuf format so that network boundary nodes can send them
///    over the wire.
#[derive(Debug)]
pub struct DistributedExec {
    /// [ExecutionPlan] exposed through [`ExecutionPlan::children`] and used as the input to
    /// execution.
    ///
    /// Initially, this is the plan present before execution:
    /// - If the plan was distributed statically, this will be the final distributed plan with all
    ///   the appropriate network boundaries in it.
    /// - If the plan is going to be distributed dynamically during execution, this is the initial
    ///   non-distributed plan.
    ///
    /// Post-execution rewrites replace this plan in the returned clone while leaving the original
    /// [`DistributedExec`] unchanged.
    base_plan: Arc<dyn ExecutionPlan>,
    /// Complete plans produced during static or dynamic preparation.
    prepared_plan: Arc<OnceLock<PreparedPlan>>,
    /// DataFusion metrics.
    metrics: ExecutionPlanMetricsSet,
    /// Storage where metrics collected from workers at runtime will place their results as they
    /// finish their respective remote tasks.
    pub(crate) metrics_store: Option<Arc<MetricsStore>>,
    /// Storage for the completed dynamic filters reported by each worker task.
    pub(crate) completed_dynamic_filter_store: Option<Arc<Store<TaskCompletedDynamicFilters>>>,
}

#[derive(Debug, Clone)]
pub(super) struct PreparedPlan {
    /// The coordinator-side plan prepared for execution.
    pub(super) head_stage: Arc<dyn ExecutionPlan>,
    /// The complete distributed plan reconstructed for visualization, including all stages.
    pub(super) plan_for_viz: Arc<dyn ExecutionPlan>,
}

impl DistributedExec {
    pub fn new(base_plan: Arc<dyn ExecutionPlan>) -> Self {
        Self {
            base_plan,
            prepared_plan: Arc::new(OnceLock::new()),
            metrics: ExecutionPlanMetricsSet::new(),
            metrics_store: None,
            completed_dynamic_filter_store: None,
        }
    }

    /// Enables task metrics collection from remote workers.
    pub fn with_metrics_collection(mut self, enabled: bool) -> Self {
        self.metrics_store = match enabled {
            true => Some(Arc::new(MetricsStore::new())),
            false => None,
        };
        self
    }

    /// Enables collection of completed dynamic filters from remote workers for display.
    pub fn with_dynamic_filter_collection(mut self, enabled: bool) -> Self {
        self.completed_dynamic_filter_store = match enabled {
            true => Some(Arc::new(Store::new())),
            false => None,
        };
        self
    }

    /// Subscribe to coalesced metrics-change notifications. Returns `None` when collection
    /// is disabled. Subscribe on the original plan, before or during execution, then call
    /// [`crate::snapshot_distributed_plan_with_metrics`] after each notification.
    ///
    /// Emits an initial notification, then covers preparation, received reports, closure, and changes to
    /// local coordinator metrics. Local sampling uses the query's reporting interval;
    /// interval zero leaves only report/lifecycle notifications. Slow consumers retain one
    /// notification, not a queue of plans. This stream does not drive query execution and
    /// remains open while the plan's metrics store exists; drop it when results end.
    pub fn metrics_updates(&self) -> Option<BoxStream<'static, ()>> {
        Some(self.metrics_store.as_ref()?.updates())
    }

    /// Waits until all worker tasks have reported their metrics back via the coordinator channel
    /// if metrics collection is enabled.
    pub async fn wait_for_metrics(&self) -> Option<HashMap<TaskKey, TaskMetrics>> {
        let task_metrics = self.metrics_store.as_ref()?;
        let plan = &self.prepared_plan.get()?.plan_for_viz;
        Some(task_metrics.wait_for(&task_keys_for_plan(plan)).await)
    }

    /// Waits until all worker tasks have reported their completed dynamic filters back via
    /// the coordinator channel if dynamic filter collection is enabled.
    pub(crate) async fn wait_for_dynamic_filters(
        &self,
    ) -> Option<HashMap<TaskKey, TaskCompletedDynamicFilters>> {
        let store = self.completed_dynamic_filter_store.as_ref()?;
        let plan = &self.prepared_plan.get()?.plan_for_viz;
        Some(store.wait_for(&task_keys_for_plan(plan)).await)
    }

    fn prepared_plan(&self) -> Result<PreparedPlan> {
        self.prepared_plan.get().cloned().ok_or_else(|| {
            internal_datafusion_err!("No prepared plan found. Was execute() called?")
        })
    }

    /// Returns the plan reconstructed during preparation for visualization and rewriting.
    pub(crate) fn plan_for_viz(&self) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self.prepared_plan()?.plan_for_viz)
    }

    /// Returns the prepared visualization plan when available, or the original optimized plan
    /// before execution has prepared one.
    pub(crate) fn plan_for_viz_or_base_plan(&self) -> Arc<dyn ExecutionPlan> {
        self.prepared_plan
            .get()
            .map(|prepared| Arc::clone(&prepared.plan_for_viz))
            .unwrap_or_else(|| Arc::clone(&self.base_plan))
    }

    /// Returns the coordinator-side plan executed by [`DistributedExec`].
    ///
    /// Unlike [`Self::plan_for_viz`], this contains [`Stage::Remote`] boundaries instead of the
    /// remote execution-plan nodes. It also retains the original plan-node instances whose
    /// metrics were populated during execution.
    ///
    /// [`Stage::Remote`]: crate::stage::Stage::Remote
    pub(crate) fn head_stage(&self) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self.prepared_plan()?.head_stage)
    }

    /// Returns a new [`DistributedExec`] with an updated visualization plan while leaving its
    /// public child unchanged.
    pub(crate) fn with_plan_for_viz(
        &self,
        plan_for_viz: Arc<dyn ExecutionPlan>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.with_plan_for_viz_and_metrics(plan_for_viz, self.metrics.clone())
    }

    pub(crate) fn with_plan_for_viz_and_metrics(
        &self,
        plan_for_viz: Arc<dyn ExecutionPlan>,
        metrics: ExecutionPlanMetricsSet,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let mut prepared_plan = self.prepared_plan()?;
        prepared_plan.plan_for_viz = plan_for_viz;
        Ok(Arc::new(Self {
            base_plan: Arc::clone(&self.base_plan),
            prepared_plan: Arc::new(OnceLock::from(prepared_plan)),
            metrics,
            metrics_store: self.metrics_store.clone(),
            completed_dynamic_filter_store: self.completed_dynamic_filter_store.clone(),
        }))
    }
}

impl DisplayAs for DistributedExec {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "DistributedExec")
    }
}

impl ExecutionPlan for DistributedExec {
    fn name(&self) -> &str {
        "DistributedExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        self.base_plan.properties()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.base_plan]
    }

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let child = require_one_child(&children)?;
        // Replacing the public child is independent from replacing the visualization plan. A
        // post-execution rewrite updates the latter explicitly via `Self::with_plan_for_viz`.
        let prepared_plan = self
            .prepared_plan
            .get()
            .cloned()
            .map_or_else(OnceLock::new, OnceLock::from);
        Ok(Arc::new(DistributedExec {
            base_plan: child,
            prepared_plan: Arc::new(prepared_plan),
            metrics: self.metrics.clone(),
            metrics_store: self.metrics_store.clone(),
            completed_dynamic_filter_store: self.completed_dynamic_filter_store.clone(),
        }))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if partition > 0 {
            // The DistributedExec node calls try_assign_urls() lazily upon calling .execute(). This means
            // that .execute() must only be called once, as we cannot afford to perform several
            // random URL assignation while calling multiple partitions, as they will differ,
            // producing an invalid plan
            return exec_err!(
                "DistributedExec must only have 1 partition, but it was called with partition index {partition}"
            );
        }

        let base_plan = Arc::clone(&self.base_plan);
        let prepared_plan = Arc::clone(&self.prepared_plan);
        let collect_dynamic_filters = self.completed_dynamic_filter_store.is_some();
        let metrics_store = self.metrics_store.clone();

        let mut builder = RecordBatchReceiverStreamBuilder::new(self.schema(), 1);
        let tx = builder.tx();
        let query_coordinator = Arc::new(QueryCoordinator::new(
            Arc::clone(&context),
            &self.metrics,
            self.metrics_store.clone(),
            self.completed_dynamic_filter_store.clone(),
            &mut builder,
        ));
        // Capture the guard before spawning so even an unpolled execution future cancels on drop.
        let guard = query_coordinator.spawner.end_query_guard();
        builder.spawn(async move {
            // Dropping this `guard` is what signals the coordinator->worker channel to be dropped,
            // which triggers a chain reaction that ends up also gracefully closing the
            // worker->coordinator channel. The flow looks like this:
            // 1. Execution ends normally. All Arrow RecordBatches are emitted (note that
            //    the response stream does not terminate yet).
            // 2. The `guard` here is dropped, signalling the coordinator->worker streams to finish.
            // 3. The worker observes end-of-stream in `impl_coordinator_channel.rs`.
            // 4. The worker sends final metrics etc., if enabled.
            // 5. The tasks owned by the response stream finish, such as tasks collecting metrics etc.
            // 6. The response stream finishes, ending the query for the user.
            let _guard = guard;

            let d_cfg = DistributedConfig::from_config_options(context.session_config().options())?;
            let mut prepared = match d_cfg.dynamic_task_count {
                true => prepare_dynamic_plan(&query_coordinator, &base_plan).await?,
                false => prepare_static_plan(&query_coordinator, &base_plan).await?,
            };

            let dynamic_filtering_enabled =
                is_local_dynamic_filtering_enabled(context.session_config());
            prepared.plan_for_viz = match dynamic_filtering_enabled && collect_dynamic_filters {
                true => sever_dynamic_filter_relationships_in_plan_for_display(
                    prepared.plan_for_viz,
                    &context,
                )?,
                false => prepared.plan_for_viz,
            };
            let head_stage = Arc::clone(&prepared.head_stage);
            prepared_plan.set(prepared).map_err(|_| {
                internal_datafusion_err!("DistributedExec was already prepared for execution")
            })?;
            if let Some(store) = metrics_store {
                store.notify();
                if d_cfg.metrics_reporting_interval_ms > 0 {
                    let plan = Arc::clone(&head_stage);
                    let interval = Duration::from_millis(d_cfg.metrics_reporting_interval_ms);
                    let finished = query_coordinator.spawner.query_finished();
                    query_coordinator.spawner.spawn(async move {
                        tokio::select! {
                            _ = finished.cancelled() => Ok(()),
                            result = report_local_metrics(plan, store, interval) => result,
                        }
                    });
                }
            }
            let mut stream = head_stage.execute(partition, context)?;
            while let Some(msg) = stream.next().await {
                if tx.send(Ok(msg?)).await.is_err() {
                    break; // channel closed
                }
            }
            Ok(())
        });

        Ok(builder.build())
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
}

async fn report_local_metrics(
    plan: Arc<dyn ExecutionPlan>,
    store: Arc<MetricsStore>,
    interval: Duration,
) -> Result<()> {
    let mut ticker = interval_at(Instant::now() + interval, interval);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let mut previous = Vec::new();
    loop {
        ticker.tick().await;
        let current = collect_plan_metrics(&plan)?
            .iter()
            .map(snapshot_metrics)
            .collect::<Vec<_>>();
        if current.len() != previous.len()
            || current
                .iter()
                .zip(&previous)
                .any(|(a, b)| !metrics_equal(a, b))
        {
            previous = current;
            store.notify();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::DistributedExt;
    use crate::test_utils::mock_exec::MockExec;
    use crate::{DistributedMetricsFormat, snapshot_distributed_plan_with_metrics};
    use datafusion::arrow::datatypes::Schema;
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::physical_expr::projection::ProjectionExpr;
    use datafusion::physical_plan::execute_stream;
    use datafusion::physical_plan::metrics::MetricValue;
    use datafusion::physical_plan::projection::ProjectionExec;
    use datafusion::prelude::{SessionConfig, SessionContext};
    use futures::TryStreamExt;
    use std::sync::atomic::{AtomicBool, Ordering};
    use tokio::sync::Notify;
    use tokio::time::timeout;

    #[tokio::test]
    async fn local_metrics_notify_without_remote_reports_and_stop_at_query_end() -> Result<()> {
        let schema = Arc::new(Schema::empty());
        let open = Arc::new(AtomicBool::new(false));
        let gate = Arc::new(Notify::new());
        let source = MockExec::new(
            vec![Ok(RecordBatch::new_empty(Arc::clone(&schema)))],
            schema,
        )
        .with_use_task(false)
        .with_gate(Arc::clone(&open), Arc::clone(&gate));
        let source: Arc<dyn ExecutionPlan> = Arc::new(ProjectionExec::try_new(
            Vec::<ProjectionExpr>::new(),
            Arc::new(source),
        )?);
        let plan: Arc<dyn ExecutionPlan> =
            Arc::new(DistributedExec::new(Arc::clone(&source)).with_metrics_collection(true));
        let ctx = SessionContext::new_with_config(
            SessionConfig::new().with_distributed_option_extension(DistributedConfig {
                dynamic_task_count: false,
                metrics_reporting_interval_ms: 10,
                ..Default::default()
            }),
        );
        let mut updates = plan
            .downcast_ref::<DistributedExec>()
            .unwrap()
            .metrics_updates()
            .unwrap();
        assert_eq!(updates.next().await, Some(()));
        let mut results = execute_stream(Arc::clone(&plan), ctx.task_ctx())?;
        tokio::select! {
            result = results.next() => panic!("gated query returned: {result:?}"),
            notification = timeout(Duration::from_secs(2), updates.next()) => {
                assert_eq!(notification.unwrap(), Some(()));
            },
        }
        let rows = source
            .metrics()
            .unwrap()
            .iter()
            .find_map(|metric| match metric.value() {
                MetricValue::OutputRows(rows) => Some(rows.clone()),
                _ => None,
            })
            .unwrap();
        let format = DistributedMetricsFormat::Aggregated;
        let first = snapshot_distributed_plan_with_metrics(Arc::clone(&plan), format)?;
        rows.add(7);
        timeout(Duration::from_secs(2), async {
            loop {
                updates.next().await.unwrap();
                let snapshot =
                    snapshot_distributed_plan_with_metrics(Arc::clone(&plan), format).unwrap();
                if snapshot.plan.children()[0].metrics().unwrap().output_rows() == Some(7) {
                    break;
                }
            }
        })
        .await
        .unwrap();
        assert_eq!(
            first.plan.children()[0].metrics().unwrap().output_rows(),
            Some(0)
        );
        open.store(true, Ordering::SeqCst);
        gate.notify_waiters();
        timeout(Duration::from_secs(2), results.try_collect::<Vec<_>>())
            .await
            .unwrap()?;
        Ok(())
    }
}
