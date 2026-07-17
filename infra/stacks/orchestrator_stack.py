from aws_cdk import (
    CfnOutput,
    Duration,
    Stack,
)
from aws_cdk import aws_dynamodb as dynamodb
from aws_cdk import aws_lambda as lambda_
from aws_cdk import aws_stepfunctions as sfn
from aws_cdk import aws_stepfunctions_tasks as sfn_tasks
from constructs import Construct


class HdpOrchestratorStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        account = Stack.of(self).account
        region = Stack.of(self).region

        build_worklist_lambda = lambda_.Function.from_function_name(
            self,
            "BuildWorklistLambdaRef",
            "hdp-build-worklist",
        )
        summarize_run_lambda = lambda_.Function.from_function_name(
            self,
            "SummarizeRunLambdaRef",
            "hdp-summarize-run",
        )

        run_history_table = dynamodb.Table.from_table_name(
            self,
            "RunHistoryTableRef",
            "hdp-run-history",
        )
        watermark_table = dynamodb.Table.from_table_name(
            self,
            "WatermarkTableRef",
            "hdp-watermarks",
        )

        run_batch_workflow_name = "hdp-phase2-state-machine"
        job_queue_arn = f"arn:aws:batch:{region}:{account}:job-queue/hdp-phase2-queue"

        pull_job_definition_arn = (
            f"arn:aws:batch:{region}:{account}:job-definition/hdp-pull-job"
        )
        clean_job_definition_arn = (
            f"arn:aws:batch:{region}:{account}:job-definition/hdp-clean-job"
        )
        qaqc_job_definition_arn = (
            f"arn:aws:batch:{region}:{account}:job-definition/hdp-qaqc-job"
        )
        merge_job_definition_arn = (
            f"arn:aws:batch:{region}:{account}:job-definition/hdp-merge-job"
        )
        stationlist_job_definition_arn = (
            f"arn:aws:batch:{region}:{account}:job-definition/hdp-stationlist-job"
        )

        def noop_condition(error_path: str) -> sfn.Condition:
            return sfn.Condition.or_(
                sfn.Condition.string_matches(error_path, '*"ExitCode":3*'),
                sfn.Condition.string_matches(error_path, '*"ExitCode": 3*'),
                sfn.Condition.string_matches(error_path, '*"exitCode":3*'),
                sfn.Condition.string_matches(error_path, '*"exitCode": 3*'),
            )

        def add_transient_retry(task: sfn_tasks.BatchSubmitJob) -> None:
            task.add_retry(
                errors=[
                    "Batch.ClientException",
                    "Batch.ServerException",
                    "States.Timeout",
                    "States.HeartbeatTimeout",
                ],
                interval=Duration.seconds(30),
                max_attempts=2,
                backoff_rate=2.0,
            )

        def history_update(
            state_id: str,
            stage: str,
            status: str,
            *,
            error_path: str | None = None,
            final: bool = False,
            failed: bool = False,
        ) -> sfn_tasks.DynamoUpdateItem:
            expression_names = {
                "#stage_status": f"{stage}_status",
                "#stage_error": f"{stage}_error",
                "#network": "network",
                "#updated_at": "updated_at",
            }
            expression_values = {
                ":status": sfn_tasks.DynamoAttributeValue.from_string(status),
                ":network": sfn_tasks.DynamoAttributeValue.from_string(
                    sfn.JsonPath.string_at("$.network")
                ),
                ":updated_at": sfn_tasks.DynamoAttributeValue.from_string(
                    sfn.JsonPath.string_at("$$.State.EnteredTime")
                ),
            }

            if error_path:
                expression_values[":error"] = (
                    sfn_tasks.DynamoAttributeValue.from_string(
                        sfn.JsonPath.string_at(error_path)
                    )
                )
            else:
                expression_values[":error"] = (
                    sfn_tasks.DynamoAttributeValue.from_string("")
                )

            update_expression = (
                "SET #stage_status = :status, #stage_error = :error, "
                "#network = if_not_exists(#network, :network), #updated_at = :updated_at"
            )

            if final:
                expression_names["#final_status"] = "final_status"
                expression_names["#final_stage"] = "final_stage"
                expression_names["#failed"] = "failed"
                expression_values[":final_status"] = (
                    sfn_tasks.DynamoAttributeValue.from_string(status)
                )
                expression_values[":final_stage"] = (
                    sfn_tasks.DynamoAttributeValue.from_string(stage)
                )
                expression_values[":failed"] = (
                    sfn_tasks.DynamoAttributeValue.from_string(
                        "true" if failed else "false"
                    )
                )
                update_expression += (
                    ", #final_status = :final_status, "
                    "#final_stage = :final_stage, #failed = :failed"
                )

            return sfn_tasks.DynamoUpdateItem(
                self,
                state_id,
                table=run_history_table,
                key={
                    "run_id": sfn_tasks.DynamoAttributeValue.from_string(
                        sfn.JsonPath.string_at("$.run_id")
                    ),
                    "station_id": sfn_tasks.DynamoAttributeValue.from_string(
                        sfn.JsonPath.string_at("$.station_id")
                    ),
                },
                expression_attribute_names=expression_names,
                expression_attribute_values=expression_values,
                update_expression=update_expression,
                result_path=sfn.JsonPath.DISCARD,
            )

        def station_result(
            state_id: str,
            stage: str,
            status: str,
            failed: bool,
        ) -> sfn.Pass:
            return sfn.Pass(
                self,
                state_id,
                parameters={
                    "station_id.$": "$.station_id",
                    "network.$": "$.network",
                    "run_id.$": "$.run_id",
                    "final_stage": stage,
                    "final_status": status,
                    "failed": failed,
                },
                result_path="$",
            )

        build_worklist = sfn_tasks.LambdaInvoke(
            self,
            "BuildWorklist",
            lambda_function=build_worklist_lambda,
            payload_response_only=True,
            result_path="$",
        )

        pull_stage = sfn_tasks.BatchSubmitJob(
            self,
            "PullStage",
            job_definition_arn=pull_job_definition_arn,
            job_name="hdp-pull",
            job_queue_arn=job_queue_arn,
            container_overrides=sfn_tasks.BatchContainerOverrides(
                command=[
                    "bash",
                    "-c",
                    (
                        "START_YEAR=1980; "
                        'if [ -n "$LAST_TIMESTAMP" ]; then START_YEAR="${LAST_TIMESTAMP%%-*}"; fi; '
                        "cd /app/scripts/1_pull_data && "
                        'python GHCNh_pull.py --station "$STATION_ID" '
                        '--start-year "$START_YEAR" --end-year "$(date -u +%Y)"'
                    ),
                ],
                environment={
                    "STATION_ID": sfn.JsonPath.string_at("$.station_id"),
                    "LAST_TIMESTAMP": sfn.JsonPath.string_at("$.last_timestamp"),
                },
            ),
            integration_pattern=sfn.IntegrationPattern.RUN_JOB,
            result_path="$.pull_result",
            attempts=1,
            task_timeout=sfn.Timeout.duration(Duration.hours(2)),
        )
        add_transient_retry(pull_stage)

        pull_success = history_update("PullRecordSuccess", "pull", "success")
        pull_noop = history_update("PullRecordNoop", "pull", "no-op", final=True)
        pull_failure = history_update(
            "PullRecordFailure",
            "pull",
            "failure",
            error_path="$.pull_error.Cause",
            final=True,
            failed=True,
        )
        pull_done_noop = station_result("PullDoneNoop", "pull", "no-op", False)
        pull_done_failed = station_result("PullDoneFailed", "pull", "failure", True)

        clean_stage = sfn_tasks.BatchSubmitJob(
            self,
            "CleanStage",
            job_definition_arn=clean_job_definition_arn,
            job_name="hdp-clean",
            job_queue_arn=job_queue_arn,
            container_overrides=sfn_tasks.BatchContainerOverrides(
                command=[
                    "bash",
                    "-c",
                    '/app/docker/entrypoint.sh run-clean --station "$STATION_ID" --append',
                ],
                environment={
                    "STATION_ID": sfn.JsonPath.string_at("$.station_id"),
                },
            ),
            integration_pattern=sfn.IntegrationPattern.RUN_JOB,
            result_path="$.clean_result",
            attempts=1,
            task_timeout=sfn.Timeout.duration(Duration.hours(2)),
        )
        add_transient_retry(clean_stage)

        clean_success = history_update("CleanRecordSuccess", "clean", "success")
        clean_noop = history_update("CleanRecordNoop", "clean", "no-op", final=True)
        clean_failure = history_update(
            "CleanRecordFailure",
            "clean",
            "failure",
            error_path="$.clean_error.Cause",
            final=True,
            failed=True,
        )
        clean_done_noop = station_result("CleanDoneNoop", "clean", "no-op", False)
        clean_done_failed = station_result("CleanDoneFailed", "clean", "failure", True)

        qaqc_stage = sfn_tasks.BatchSubmitJob(
            self,
            "QaqcStage",
            job_definition_arn=qaqc_job_definition_arn,
            job_name="hdp-qaqc",
            job_queue_arn=job_queue_arn,
            container_overrides=sfn_tasks.BatchContainerOverrides(
                command=[
                    "bash",
                    "-c",
                    '/app/docker/entrypoint.sh run-qaqc --station "$STATION_ID" --append',
                ],
                environment={
                    "STATION_ID": sfn.JsonPath.string_at("$.station_id"),
                },
            ),
            integration_pattern=sfn.IntegrationPattern.RUN_JOB,
            result_path="$.qaqc_result",
            attempts=1,
            task_timeout=sfn.Timeout.duration(Duration.hours(2)),
        )
        add_transient_retry(qaqc_stage)

        qaqc_success = history_update("QaqcRecordSuccess", "qaqc", "success")
        qaqc_noop = history_update("QaqcRecordNoop", "qaqc", "no-op", final=True)
        qaqc_failure = history_update(
            "QaqcRecordFailure",
            "qaqc",
            "failure",
            error_path="$.qaqc_error.Cause",
            final=True,
            failed=True,
        )
        qaqc_done_noop = station_result("QaqcDoneNoop", "qaqc", "no-op", False)
        qaqc_done_failed = station_result("QaqcDoneFailed", "qaqc", "failure", True)

        merge_stage = sfn_tasks.BatchSubmitJob(
            self,
            "MergeStage",
            job_definition_arn=merge_job_definition_arn,
            job_name="hdp-merge",
            job_queue_arn=job_queue_arn,
            container_overrides=sfn_tasks.BatchContainerOverrides(
                command=[
                    "bash",
                    "-c",
                    '/app/docker/entrypoint.sh run-merge --station "$STATION_ID" --append',
                ],
                environment={
                    "STATION_ID": sfn.JsonPath.string_at("$.station_id"),
                },
            ),
            integration_pattern=sfn.IntegrationPattern.RUN_JOB,
            result_path="$.merge_result",
            attempts=1,
            task_timeout=sfn.Timeout.duration(Duration.hours(2)),
        )
        add_transient_retry(merge_stage)

        merge_success = history_update(
            "MergeRecordSuccess",
            "merge",
            "success",
            final=True,
        )
        merge_noop = history_update("MergeRecordNoop", "merge", "no-op", final=True)
        merge_failure = history_update(
            "MergeRecordFailure",
            "merge",
            "failure",
            error_path="$.merge_error.Cause",
            final=True,
            failed=True,
        )

        update_watermark = sfn_tasks.DynamoUpdateItem(
            self,
            "UpdateWatermark",
            table=watermark_table,
            key={
                "station_id": sfn_tasks.DynamoAttributeValue.from_string(
                    sfn.JsonPath.string_at("$.station_id")
                ),
            },
            expression_attribute_names={
                "#last_timestamp": "last_timestamp",
                "#updated_at": "updated_at",
                "#network": "network",
            },
            expression_attribute_values={
                ":last_timestamp": sfn_tasks.DynamoAttributeValue.from_string(
                    sfn.JsonPath.string_at("$$.State.EnteredTime")
                ),
                ":updated_at": sfn_tasks.DynamoAttributeValue.from_string(
                    sfn.JsonPath.string_at("$$.State.EnteredTime")
                ),
                ":network": sfn_tasks.DynamoAttributeValue.from_string(
                    sfn.JsonPath.string_at("$.network")
                ),
            },
            update_expression=(
                "SET #last_timestamp = :last_timestamp, #updated_at = :updated_at, "
                "#network = if_not_exists(#network, :network)"
            ),
            result_path=sfn.JsonPath.DISCARD,
        )

        merge_done_success = station_result(
            "MergeDoneSuccess", "merge", "success", False
        )
        merge_done_noop = station_result("MergeDoneNoop", "merge", "no-op", False)
        merge_done_failed = station_result("MergeDoneFailed", "merge", "failure", True)

        pull_failure_router = sfn.Choice(self, "PullFailureRouter")
        pull_failure_router.when(
            noop_condition("$.pull_error.Cause"),
            pull_noop.next(pull_done_noop),
        ).otherwise(pull_failure.next(pull_done_failed))
        pull_stage.add_catch(pull_failure_router, result_path="$.pull_error")

        clean_failure_router = sfn.Choice(self, "CleanFailureRouter")
        clean_failure_router.when(
            noop_condition("$.clean_error.Cause"),
            clean_noop.next(clean_done_noop),
        ).otherwise(clean_failure.next(clean_done_failed))
        clean_stage.add_catch(clean_failure_router, result_path="$.clean_error")

        qaqc_failure_router = sfn.Choice(self, "QaqcFailureRouter")
        qaqc_failure_router.when(
            noop_condition("$.qaqc_error.Cause"),
            qaqc_noop.next(qaqc_done_noop),
        ).otherwise(qaqc_failure.next(qaqc_done_failed))
        qaqc_stage.add_catch(qaqc_failure_router, result_path="$.qaqc_error")

        merge_failure_router = sfn.Choice(self, "MergeFailureRouter")
        merge_failure_router.when(
            noop_condition("$.merge_error.Cause"),
            merge_noop.next(merge_done_noop),
        ).otherwise(merge_failure.next(merge_done_failed))
        merge_stage.add_catch(merge_failure_router, result_path="$.merge_error")

        per_station_chain = (
            sfn.Chain.start(pull_stage)
            .next(pull_success)
            .next(clean_stage)
            .next(clean_success)
            .next(qaqc_stage)
            .next(qaqc_success)
            .next(merge_stage)
            .next(update_watermark)
            .next(merge_success)
            .next(merge_done_success)
        )

        process_stations = sfn.Map(
            self,
            "ProcessStations",
            items_path="$.work_items",
            result_path="$.station_results",
            max_concurrency=500,
            item_selector={
                "run_id.$": "$.run_id",
                "network.$": "$$.Map.Item.Value.network",
                "station_id.$": "$$.Map.Item.Value.station_id",
                "last_timestamp.$": "$$.Map.Item.Value.last_timestamp",
            },
        )
        process_stations.item_processor(per_station_chain)

        stationlist_update = sfn_tasks.BatchSubmitJob(
            self,
            "StationlistUpdate",
            job_definition_arn=stationlist_job_definition_arn,
            job_name="hdp-stationlist-update",
            job_queue_arn=job_queue_arn,
            container_overrides=sfn_tasks.BatchContainerOverrides(
                command=[
                    "bash",
                    "-c",
                    (
                        "cd /app/scripts/4_merge_data && "
                        "python stnlist_update_merge.py ASOSAWOS"
                    ),
                ],
            ),
            integration_pattern=sfn.IntegrationPattern.RUN_JOB,
            result_path=sfn.JsonPath.DISCARD,
            attempts=1,
            task_timeout=sfn.Timeout.duration(Duration.hours(2)),
        )
        add_transient_retry(stationlist_update)

        notify = sfn_tasks.LambdaInvoke(
            self,
            "Notify",
            lambda_function=summarize_run_lambda,
            payload=sfn.TaskInput.from_object(
                {
                    "run_id.$": "$.run_id",
                    "network.$": "$.network",
                }
            ),
            payload_response_only=True,
            result_path="$.summary",
        )

        seed_empty_results = sfn.Pass(
            self,
            "SeedEmptyResults",
            result=sfn.Result.from_array([]),
            result_path="$.station_results",
        )

        render_results_json = sfn.Pass(
            self,
            "RenderResultsJson",
            parameters={
                "run_id.$": "$.run_id",
                "network.$": "$.network",
                "station_results.$": "$.station_results",
                "summary.$": "$.summary",
                "station_results_json.$": "States.JsonToString($.station_results)",
            },
            result_path="$",
        )

        fail_for_investigation = sfn.Fail(
            self,
            "FailForInvestigation",
            cause="One or more stations had non-transient failures.",
            error="StationFailure",
        )

        workflow_succeeded = sfn.Succeed(
            self,
            "WorkflowSucceeded",
            comment="Run completed with no non-transient station failures.",
        )

        any_station_failed = sfn.Choice(self, "AnyStationFailed")
        any_station_failed.when(
            sfn.Condition.or_(
                sfn.Condition.string_matches(
                    "$.station_results_json", '*"failed":true*'
                ),
                sfn.Condition.string_matches(
                    "$.station_results_json", '*"failed": true*'
                ),
            ),
            fail_for_investigation,
        ).otherwise(workflow_succeeded)

        notify.next(render_results_json).next(any_station_failed)

        has_work = sfn.Choice(self, "HasWorkItems")
        has_work.when(
            sfn.Condition.is_present("$.work_items[0]"),
            process_stations.next(stationlist_update).next(notify),
        ).otherwise(seed_empty_results.next(notify))

        definition = sfn.Chain.start(build_worklist).next(has_work)

        state_machine = sfn.StateMachine(
            self,
            "HdpPhase2StateMachine",
            state_machine_name=run_batch_workflow_name,
            definition_body=sfn.DefinitionBody.from_chainable(definition),
            timeout=Duration.hours(24),
        )

        CfnOutput(self, "StateMachineArn", value=state_machine.state_machine_arn)
