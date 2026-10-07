use datafusion::physical_plan::metrics::{Metric, MetricsSet};
use std::sync::Arc;

/// Copy values, not their live atomic handles, so local and remote snapshots behave alike.
pub(crate) fn snapshot_metrics(metrics: &MetricsSet) -> MetricsSet {
    let mut snapshot = MetricsSet::new();
    for metric in metrics.iter() {
        let mut value = metric.value().new_empty();
        value.aggregate(metric.value());
        let mut frozen =
            Metric::new_with_labels(value, metric.partition(), metric.labels().to_vec())
                .with_type(metric.metric_type());
        if let Some(category) = metric.metric_category() {
            frozen = frozen.with_category(category);
        }
        snapshot.push(Arc::new(frozen));
    }
    snapshot
}

pub(crate) fn metrics_equal(a: &MetricsSet, b: &MetricsSet) -> bool {
    a.iter().count() == b.iter().count()
        && a.iter().zip(b.iter()).all(|(a, b)| {
            a.partition() == b.partition() && a.labels() == b.labels() && a.value() == b.value()
        })
}
