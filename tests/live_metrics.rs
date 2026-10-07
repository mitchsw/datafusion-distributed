#[cfg(all(feature = "integration", test))]
mod tests {
    use datafusion::common::Result;
    use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
    use datafusion::execution::SessionState;
    use datafusion::physical_plan::{ExecutionPlan, execute_stream};
    use datafusion_distributed::test_utils::localhost::start_localhost_context;
    use datafusion_distributed::test_utils::test_work_unit_feed::{
        RowGeneratorExec, TestWorkUnitFeedExecCodec, TestWorkUnitFeedFunction,
        row_generator_desired_task_count_handler, row_generator_scale_up_leaf_node_handler,
    };
    use datafusion_distributed::{
        DistributedExec, DistributedExt, DistributedMetricsFormat, NetworkBoundaryExt,
        WorkerQueryContext, display_plan_ascii, rewrite_distributed_plan_with_metrics,
        snapshot_distributed_plan_with_metrics,
    };
    use futures::{StreamExt, TryStreamExt};
    use std::sync::Arc;
    use std::time::Duration;
    use test_case::test_case;
    use tokio::time::{sleep, timeout};

    #[test_case(DistributedMetricsFormat::Aggregated, false; "aggregated")]
    #[test_case(DistributedMetricsFormat::PerTask, false; "per_task")]
    #[test_case(DistributedMetricsFormat::Aggregated, true; "cancelled")]
    #[tokio::test]
    async fn live_metrics_before_results(
        format: DistributedMetricsFormat,
        cancel: bool,
    ) -> Result<()> {
        let (mut ctx, _guard, workers) = start_localhost_context(2, worker_session).await;
        ctx.set_distributed_work_unit_feed(|p: &RowGeneratorExec| Some(&p.feed));
        ctx.set_distributed_user_codec(TestWorkUnitFeedExecCodec);
        ctx.set_distributed_desired_task_count_handler(row_generator_desired_task_count_handler);
        ctx.set_distributed_scale_up_leaf_node_handler(row_generator_scale_up_leaf_node_handler);
        ctx.register_udtf("test_work_unit", Arc::new(TestWorkUnitFeedFunction));
        ctx.sql("SET distributed.metrics_reporting_interval_ms = 20")
            .await?;
        let plan = ctx.sql("SELECT letter, count(*) FROM test_work_unit('live', 2, 'rows(100),wait(3000),rows(100)', 'rows(100)') GROUP BY letter ORDER BY letter")
            .await?.create_physical_plan().await?;
        assert!(snapshot_distributed_plan_with_metrics(Arc::clone(&plan), format).is_err());
        let mut stream = execute_stream(Arc::clone(&plan), ctx.task_ctx())?;
        let snapshot = tokio::select! {
            result = stream.try_next() => panic!("query returned before live metrics: {result:?}"),
            snapshot = timeout(Duration::from_secs(2), wait_for_remote_rows(&plan, format)) => snapshot.unwrap(),
        };
        assert!(!snapshot.is_complete);
        let mut stages = 0;
        snapshot.plan.apply(|node| {
            stages += usize::from(node.is_network_boundary());
            Ok(TreeNodeRecursion::Continue)
        })?;
        assert!(stages >= 2, "expected more than coordinator and leaves");
        let dist = plan.downcast_ref::<DistributedExec>().unwrap();
        assert!(
            timeout(Duration::from_millis(20), dist.wait_for_metrics())
                .await
                .is_err()
        );

        if cancel {
            drop(stream);
        } else {
            stream.try_collect::<Vec<_>>().await?;
        }
        let final_plan = timeout(
            Duration::from_secs(5),
            rewrite_distributed_plan_with_metrics(Arc::clone(&plan), format),
        )
        .await
        .unwrap();
        let final_snapshot = snapshot_distributed_plan_with_metrics(plan, format)?;
        if cancel {
            let received_rows = output_rows(&snapshot.plan);
            assert!(output_rows(&final_snapshot.plan) >= received_rows);
        } else {
            assert!(final_snapshot.is_complete);
            assert_eq!(
                display_plan_ascii(final_plan?.as_ref(), true),
                display_plan_ascii(final_snapshot.plan.as_ref(), true)
            );
        }
        timeout(Duration::from_secs(5), async {
            loop {
                let mut running = 0;
                for worker in &workers {
                    running += worker.tasks_running().await + worker.coordinator_channels_running();
                }
                if running == 0 {
                    break;
                }
                sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        Ok(())
    }

    async fn worker_session(ctx: WorkerQueryContext) -> Result<SessionState> {
        Ok(ctx
            .builder
            .with_distributed_user_codec(TestWorkUnitFeedExecCodec)
            .build())
    }

    async fn wait_for_remote_rows(
        plan: &Arc<dyn ExecutionPlan>,
        format: DistributedMetricsFormat,
    ) -> datafusion_distributed::DistributedMetricsSnapshot {
        let mut updates = plan
            .downcast_ref::<DistributedExec>()
            .unwrap()
            .metrics_updates()
            .unwrap();
        while updates.next().await.is_some() {
            if let Ok(snapshot) = snapshot_distributed_plan_with_metrics(Arc::clone(plan), format)
                && output_rows(&snapshot.plan) > 0
            {
                return snapshot;
            }
        }
        panic!("metrics subscription closed before progress");
    }

    fn output_rows(plan: &Arc<dyn ExecutionPlan>) -> usize {
        let mut rows = 0;
        plan.apply(|node| {
            rows += node.metrics().and_then(|m| m.output_rows()).unwrap_or(0);
            Ok(TreeNodeRecursion::Continue)
        })
        .unwrap();
        rows
    }
}
