# Collecting Runtime Metrics

In vanilla DataFusion, the runtime metrics of an `ExecutionPlan` (rows produced, time spent, bytes,
etc.) live on each node and can be inspected after execution. In a distributed query most of the plan
runs on remote workers, so those metrics need to be gathered back to the coordinator before you can
display them.

Distributed DataFusion does this for you, and exposes functions you can use to build your own
EXPLAIN ANALYZE in application code.

## Enabling collection

Metrics collection across network boundaries is **on by default**. You can toggle it explicitly:

```rust
let state = SessionStateBuilder::new()
    .with_default_features()
    .with_distributed_worker_resolver(/* ... */)
    .with_distributed_planner()
    .with_distributed_metrics_collection(true) // default is true
    .build();
```

When enabled, each worker streams the metrics for its tasks back to the coordinator on a dedicated
channel, so they are not lost even if the result stream is dropped early (for example by a `LIMIT`).

## Rendering a plan with metrics

These functions, all exported from the crate root, do the work:

- `rewrite_distributed_plan_with_dynamic_filters(plan, task_ctx)` — folds the completed dynamic
  filters reported by each worker task into an isolated copy of the plan. Pass the same task context
  used to execute the plan. When displaying both dynamic filters and metrics, apply the
  dynamic-filter rewrite first.
- `rewrite_distributed_plan_with_metrics(plan, format)` — folds every task's metrics back into the
  coordinator's copy of the plan. It waits for all worker metrics to arrive, so the result is always
  complete. The `format` is a `DistributedMetricsFormat`:
    - `Aggregated` — metrics from all tasks of a stage are summed/aggregated into one value per node.
    - `PerTask` — each metric collects its per-task values into a map keyed by task id
      (`output_rows={0:.., 1:..}`) so you can see each task individually.
- `display_plan_ascii(plan, show_metrics)` — renders the plan tree. Pass `true` to include the metrics
  attached to each node.

For a complete final view, drain the result stream before awaiting the final rewrites.
For a live view, use the snapshot API described below.

```rust
use datafusion::physical_plan::execute_stream;
use datafusion_distributed::{
    DistributedMetricsFormat, display_plan_ascii,
    rewrite_distributed_plan_with_dynamic_filters, rewrite_distributed_plan_with_metrics,
};
use futures::TryStreamExt;

// 1. Plan the query.
let plan = ctx.sql(sql).await?.create_physical_plan().await?;
let task_ctx = ctx.task_ctx();

// 2. Execute it to completion (collect, or otherwise fully drain the stream).
execute_stream(plan.clone(), task_ctx.clone())?
    .try_collect::<Vec<_>>()
    .await?;

// 3. Fold the completed per-task dynamic filters back into the plan...
let plan =
    rewrite_distributed_plan_with_dynamic_filters(plan, &task_ctx).await?;

// 4. Fold the per-task metrics back into the plan...
let plan =
    rewrite_distributed_plan_with_metrics(plan, DistributedMetricsFormat::Aggregated).await?;

// 5. ...and render it.
println!("{}", display_plan_ascii(plan.as_ref(), true));
```

This produces an EXPLAIN ANALYZE that spans the whole cluster — every stage and every node carries its
runtime metrics, including network-level metrics on the boundaries:

```
┌───── DistributedExec ── plan_bytes_sent={0:8.07 KB}, plan_send_latency_avg={0:22.63ms}, ...
│ SortPreservingMergeExec: [count(*)@0 DESC], fetch=5, metrics=[output_rows=5, elapsed_compute=391.83µs, ...]
│   [Stage 2] => NetworkCoalesceExec: output_partitions=32, input_tasks=2, metrics=[elapsed_compute=5.86ms, bytes_transferred=20.1 KB, network_latency_p50=366.00µs, network_latency_p95=603.43µs, ...]
└──────────────────────────────────────────────────
  ┌───── Stage 2 ── tasks=2, partitions=16 plan_added_at={0:25.78ms}, plan_finished_at={0:38.35ms}, ...
  │ AggregateExec: mode=FinalPartitioned, gby=[MinTemp@0 as MinTemp], aggr=[count(Int64(1))], metrics=[output_rows=180, elapsed_compute=5.44ms, ...]
  │     [Stage 1] => NetworkShuffleExec: output_partitions=16, input_tasks=2, metrics=[bytes_transferred=15.0 KB, ...]
  └──────────────────────────────────────────────────
    ┌───── Stage 1 ── tasks=2, partitions=32 ...
    │ AggregateExec: mode=Partial, gby=[MinTemp@0 as MinTemp], aggr=[count(Int64(1))], metrics=[output_rows=249, ...]
    │     DistributedLeafExec: DataSourceExec: ..., metrics=[output_rows=366, bytes_scanned=5.40 K, ...]
    └──────────────────────────────────────────────────
```

> If `plan` is not a distributed plan (its root is not a `DistributedExec`),
> `rewrite_distributed_plan_with_metrics` returns it unchanged, so it is always safe to call.

## Available metrics and live progress

`snapshot_distributed_plan_with_metrics(plan, format)` reads currently available
metrics without waiting for workers. It supports `Aggregated` and `PerTask`, omits
missing reports, and returns a plan with frozen metric values plus `is_complete`.
Call on the original executing `DistributedExec` root. Before full plan
preparation it returns an error. A non-distributed root is returned unchanged.

Subscribe with `DistributedExec::metrics_updates()` and read snapshots after
notifications. Subscriptions work before execution, retain one pending change,
and do not queue plans or make network requests. Continue consuming results and
drop the subscription when they end. Collection disabled returns `None`.

To receive reports during execution, set
`SET distributed.metrics_reporting_interval_ms = 500` before planning. The default
zero preserves terminal-only reporting; `collect_metrics=false` disables all
reporting. Both ends must support the new live batch message.

Each worker/query samples changed tasks and pushes batches of cumulative
`TaskMetrics` snapshots over an existing control stream. Coordinator-local
operators are sampled at the same interval and also notify on change. Consumers
can rate-limit notifications without a separate polling loop. A task finishing
between ticks retains its completion snapshot until the next batch.

All stages report directly to the query coordinator. Each operator aggregates its
tasks using the existing rewriter; adding output rows across every stage would
count data repeatedly. Final reports replace live values. `is_complete` requires
all expected final reports, not query success. A channel closing without a final
report retains its available metrics and leaves the snapshot incomplete; disabled
collection also reports incomplete. Terminal rewriting continues to use only
terminal reports, so use snapshots after cancellation or failure.

A complete notification-driven example and design rationale are in
[the ADR](../../adr/0001-live-query-metrics.md#coordinator-example).
