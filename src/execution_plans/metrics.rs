use crate::metrics::DISTRIBUTED_DATAFUSION_TASK_ID_LABEL;
use crate::metrics::snapshot::snapshot_metrics;
use datafusion::physical_plan::metrics::MetricsSet;
use std::sync::Arc;

use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::error::Result;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::{
    ChildrenPropertiesMode, DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties,
    ReplaceChildrenOptions,
};
use delegate::delegate;
use std::fmt::{Debug, Formatter};

/// A transparent wrapper that delegates all execution to its child but returns custom metrics. This node is invisible during display.
/// The structure of a plan tree is closely tied to the [TaskMetricsRewriter].
pub(crate) struct MetricsWrapperExec {
    inner: Arc<dyn ExecutionPlan>,
    /// metrics for this plan node.
    metrics: MetricsSet,
    include_inner_metrics: bool,
}

impl MetricsWrapperExec {
    pub(crate) fn new(inner: Arc<dyn ExecutionPlan>, metrics: MetricsSet) -> Self {
        Self {
            inner,
            metrics,
            include_inner_metrics: true,
        }
    }

    /// Freeze both reported and coordinator-local metrics. A reported metric supersedes a
    /// local handle with the same identity, even if that handle has advanced since sampling.
    pub(crate) fn snapshot(inner: Arc<dyn ExecutionPlan>, metrics: MetricsSet) -> Self {
        let mut frozen = snapshot_metrics(&metrics);
        for local in snapshot_metrics(&inner.metrics().unwrap_or_default()) {
            if !metrics.iter().any(|reported| {
                reported.value().name() == local.value().name()
                    && reported.partition() == local.partition()
                    && reported
                        .labels()
                        .iter()
                        .filter(|l| l.name() != DISTRIBUTED_DATAFUSION_TASK_ID_LABEL)
                        .eq(local
                            .labels()
                            .iter()
                            .filter(|l| l.name() != DISTRIBUTED_DATAFUSION_TASK_ID_LABEL))
            }) {
                frozen.push(local);
            }
        }
        Self {
            inner,
            metrics: frozen,
            include_inner_metrics: false,
        }
    }

    #[cfg(test)]
    pub(crate) fn inner(&self) -> &Arc<dyn ExecutionPlan> {
        &self.inner
    }
}

/// MetricsWrapperExec is invisible during display.
impl DisplayAs for MetricsWrapperExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        self.inner.fmt_as(t, f)
    }
}

/// MetricsWrapperExec is visible when debugging.
impl Debug for MetricsWrapperExec {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "MetricsWrapperExec ({:?})", self.inner)
    }
}

impl ExecutionPlan for MetricsWrapperExec {
    delegate! {
        to self.inner {
            fn name(&self) -> &str;
            fn properties(&self) -> &Arc<PlanProperties>;
        }
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        self.inner.children()
    }

    fn apply_expressions(
        &self,
        f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        self.inner.apply_expressions(f)
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(MetricsWrapperExec {
            inner: Arc::clone(&self.inner).replace_children(
                children.clone(),
                ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
            )?,
            metrics: self.metrics.clone(),
            include_inner_metrics: self.include_inner_metrics,
        }))
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        unimplemented!("MetricsWrapperExec does not implement execute")
    }

    /// returns the wrapped metrics merged with any other present in
    /// the inner [ExecutionPlan].
    fn metrics(&self) -> Option<MetricsSet> {
        if !self.include_inner_metrics {
            return Some(self.metrics.clone());
        }
        match self.inner.metrics() {
            None => Some(self.metrics.clone()),
            Some(local_metrics) => {
                let mut all_metrics = self.metrics.clone();

                for local_metric in local_metrics {
                    // When a node is executed in an in-process worker, the ExecutionPlan might not
                    // have suffered any [de]serialization. This means that execution metrics
                    // collected at runtime in the Worker will be automatically visible in the
                    // coordinator, as both happen to run in-process, and therefore, the pointer
                    // that holds the ExecutionPlan MetricsSet is the same.
                    //
                    // This means that we need to dedupe the metrics here, otherwise, we might
                    // double-count metrics:
                    // 1. What was collected in the local worker
                    // 2. What is automatically available in the coordinator because of pointer
                    //    equivalence.
                    if !self.metrics.iter().any(|wrapped| {
                        wrapped.value() == local_metric.value()
                            && wrapped.partition() == local_metric.partition()
                    }) {
                        all_metrics.push(local_metric);
                    }
                }
                Some(all_metrics)
            }
        }
    }

    fn downcast_delegate(&self) -> Option<&dyn ExecutionPlan> {
        Some(self.inner.as_ref())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::Schema;
    use datafusion::physical_plan::empty::EmptyExec;
    use datafusion::physical_plan::metrics::{Count, Metric, MetricValue};

    #[test]
    fn snapshot_does_not_merge_newer_values_from_live_local_handles() {
        let count = Count::new();
        count.add(5);
        let mut metrics = MetricsSet::new();
        metrics.push(Arc::new(Metric::new(
            MetricValue::OutputRows(count.clone()),
            Some(0),
        )));
        let reported = snapshot_metrics(&metrics);
        let local: Arc<dyn ExecutionPlan> = Arc::new(MetricsWrapperExec::new(
            Arc::new(EmptyExec::new(Arc::new(Schema::empty()))),
            metrics,
        ));
        count.add(10);
        let snapshot = MetricsWrapperExec::snapshot(local, reported);
        assert_eq!(snapshot.metrics().unwrap().output_rows(), Some(5));
        count.add(20);
        assert_eq!(snapshot.metrics().unwrap().output_rows(), Some(5));
    }
}
