# Available and live query metrics

Status: proposed

Related issue: [#685: Add a non-blocking snapshot API for available distributed task metrics](https://github.com/datafusion-contrib/datafusion-distributed/issues/685)

## Problem

We want to inspect distributed query metrics without waiting for every task,
including after cancellation or failure. Issue #685 requests available reports,
missing-task support, existing output formats, and a completeness flag while
preserving the terminal API. It requires no worker or protocol changes.

This proposal also adds opt-in reporting and notifications for progress while
tasks run. The snapshot API works independently with terminal-only reporting.

## Decision

Keep reading metrics independent of reporting them:

- `snapshot_distributed_plan_with_metrics(plan, format)` synchronously returns
  `DistributedMetricsSnapshot { plan, is_complete }`. It reads received reports,
  samples local metrics, and reuses the existing stage/task rewriter. Missing
  reports are omitted, including gaps in task numbers. The returned plan exposes
  frozen values through `ExecutionPlan::metrics()`; it is not a text rendering.
- `DistributedExec::metrics_updates()` subscribes to coalesced change notifications.
  Consumers await a notification, then read a snapshot. A slow consumer retains
  one notification, not a queue of snapshots. The subscription does not execute
  the query or initiate network requests. Drop it when result consumption ends.
- `distributed.metrics_reporting_interval_ms` opts into periodic reporting;
  zero preserves terminal-only reporting. Each worker/query has one demand-driven
  reporter on an existing control stream. Changed tasks are batched as
  `MetricsUpdate(Vec<TaskMetricsUpdate>)`, where each entry contains a `TaskKey`
  and the existing `TaskMetrics` payload. Values replace the previous snapshot.
  No per-task timers or new RPCs are introduced. Slow transport skips ticks.

## Reporting and consumption

Each query has a coordinator. A worker can run several tasks from several stages
and queries; each worker/query pair shares one reporter. Collection must be
enabled with `distributed.collect_metrics` (true by default).

```text
Worker A, query Q                       Coordinator, query Q

  one periodic timer
          |
          v
  sample ExecutionPlan::metrics()
  for Q's active tasks
          |
          v
  batch changed TaskMetrics ----------> latest report per TaskKey
      periodic PUSH over an                  |
      existing control stream                | coalesced notification
                                             v
                                          consumer
                                             |
                                          snapshot()  synchronous PULL
                                             |
                                             v
                                  cached reports + local metrics
                                             |
                                             v
                                  frozen plan + is_complete
```

The snapshot call reads coordinator memory and samples local operators; it sends
no request to workers. Notifications carry `()`, not plans or metric payloads.
The consumer is the embedding application, which decides how to forward progress
to its caller.
Subscribing emits an initial notification; later notifications cover preparation,
received reports, channel closure, and changed local metrics.

Local coordinator operators are sampled at the same configured interval and
notify when their metrics change, so local work remains observable after remote
work finishes. Consumers may coalesce notifications further to cap output rate;
they do not need a separate polling timer. This does not synchronize worker
sampling: a notification means some available state changed, not that every
worker has completed a reporting round.

The first registered task's control stream carries that worker/query's batches.
Sampling runs when the stream is polled and skips missed ticks under backpressure;
it does not enqueue a report for every elapsed interval. The coordinator replaces
cached task values, and each subscriber retains one pending notification. State
scales with tasks/operators and subscribers, not the number of elapsed ticks.

## Multistage aggregation

Result batches move through the execution graph. Every stage reports its own
metrics directly to the coordinator; intermediate stages do not forward totals
from their inputs. Worker placement is independent of stage membership:

```text
Result flow (one possible five-stage query):

  S3: scan --+
             +--> S2: join --> S1: aggregate --> S0: coordinator --> caller
  S4: scan --+

Metric flow for the same query Q:

  Worker A: [S3/task 0, S2/task 1] -- batch --+
                                              |
  Worker B: [S3/task 1, S2/task 0] -- batch --+--> Q's coordinator
                                              |           |
  Worker C: [S4/task 0, S1/task 0] -- batch --+           v
                                                      cache by
                                                (query, stage, task)
                                                          |
                                            task-specific plan traversal
                                                          |
                                                          v
                                              metrics on each operator
```

The existing rewriter maps task-local preorder metrics to their operators,
including task-specialized plans. For a scan, task values 100 and 40 produce 140;
replacing 100 with 120 produces 160, not 260. A later aggregate's eight output
rows remain a separate operator metric. Other metrics retain DataFusion's
aggregation semantics, including gauges that decrease. Missing tasks are omitted,
and per-task output preserves original task IDs even when reports have gaps.

## Task completion and cancellation

```text
Worker task                         Coordinator's cached task metrics

running -- periodic sample -------> replace live values
   |
all output partitions finish/drop
   |
freeze completion snapshot
   +------ next report tick ------> replace live values
   |
query ends -- final TaskMetrics --> replace values; mark final
                                    ignore any later live report

channel closes without final -----> retain live values; mark closed
                                    still missing a final report
```

Capture a task's completion snapshot after all output partitions finish or are
dropped, and retain it for the next batch, including tasks shorter than the
reporting interval. If the query ends before that tick, the existing final-report
path still applies. Final `TaskMetrics` travels on each task's own channel; it
can race with a live batch on the carrier channel and always takes precedence.

`is_complete` requires every expected final task report; it does not indicate
query success or that local execution finished. Channel closure preserves live
values but does not imply completeness. On cancellation, a snapshot includes only
reports already received and current local metrics; no remote flush is promised.
The terminal API continues to consume terminal reports only and uses its existing
closure behavior; live values never stand in for a final report there.

## Coordinator example

Call on the original `DistributedExec` root. Subscribe before inspecting metrics
to avoid a missed notification; keep consuming the result stream concurrently.
The snapshot returns an error until the full visualization plan is prepared.

```rust
use datafusion::common::{Result, exec_datafusion_err};
use datafusion::physical_plan::execute_stream;
use datafusion::prelude::SessionContext;
use datafusion_distributed::{
    DistributedExec, DistributedMetricsFormat, display_plan_ascii,
    snapshot_distributed_plan_with_metrics,
};
use futures::{StreamExt, TryStreamExt};
use std::sync::Arc;

async fn run_with_progress(ctx: &SessionContext, sql: &str) -> Result<()> {
    ctx.sql("SET distributed.metrics_reporting_interval_ms = 500").await?;
    let plan = ctx.sql(sql).await?.create_physical_plan().await?;
    let mut updates = plan.downcast_ref::<DistributedExec>()
        .and_then(DistributedExec::metrics_updates)
        .ok_or_else(|| exec_datafusion_err!("distributed metrics are disabled"))?;
    let mut results = execute_stream(Arc::clone(&plan), ctx.task_ctx())?;
    loop {
        tokio::select! {
            batch = results.try_next() => match batch? {
                Some(batch) => println!("result rows: {}", batch.num_rows()),
                None => break,
            },
            Some(()) = updates.next() => {
                if let Ok(snapshot) = snapshot_distributed_plan_with_metrics(
                    Arc::clone(&plan), DistributedMetricsFormat::Aggregated,
                ) {
                    println!("{}", display_plan_ascii(snapshot.plan.as_ref(), true));
                }
            },
        }
    }
    Ok(())
}
```

## Tradeoffs

This provides latest available metrics, not a synchronized cluster snapshot or a
percentage-complete estimate. Reporting times differ across workers. Sampling,
retained state, and full task payloads scale with task/operator count; batching
reduces message overhead, not that underlying work. Sparse operator encoding is
deferred until measurements justify it. The metric codec's supported types are
unchanged. Enable reporting only when both ends support the new batch message.
