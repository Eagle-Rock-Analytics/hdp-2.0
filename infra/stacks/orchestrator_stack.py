from aws_cdk import (
    CfnOutput,
    Duration,
    Stack,
)
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

        run_batch_workflow_name = "hdp-phase2-state-machine"
        job_queue_arn = f"arn:aws:batch:{region}:{account}:job-queue/hdp-phase2-queue"

        clean_job_definition_arn = (
            f"arn:aws:batch:{region}:{account}:job-definition/hdp-clean-job"
        )
        qaqc_job_definition_arn = (
            f"arn:aws:batch:{region}:{account}:job-definition/hdp-qaqc-job"
        )
        merge_job_definition_arn = (
            f"arn:aws:batch:{region}:{account}:job-definition/hdp-merge-job"
        )

        build_worklist = sfn_tasks.LambdaInvoke(
            self,
            "BuildWorklist",
            lambda_function=build_worklist_lambda,
            payload_response_only=True,
            result_path="$",
        )

        worklist_empty = sfn.Succeed(
            self,
            "NoStationsToProcess",
            comment="No stale stations returned by build-worklist.",
        )

        stage_item_done = sfn.Pass(
            self,
            "StageItemDone",
            result=sfn.Result.from_object({"status": "done"}),
            result_path="$.item_result",
        )

        def noop_condition(error_path: str) -> sfn.Condition:
            return sfn.Condition.or_(
                sfn.Condition.string_matches(error_path, '*"ExitCode":3*'),
                sfn.Condition.string_matches(error_path, '*"ExitCode": 3*'),
                sfn.Condition.string_matches(error_path, '*"exitCode":3*'),
                sfn.Condition.string_matches(error_path, '*"exitCode": 3*'),
            )

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

        clean_noop = sfn.Pass(
            self,
            "CleanNoop",
            result=sfn.Result.from_object({"stage": "clean", "status": "NOOP"}),
            result_path="$.clean_result",
        )
        clean_fail = sfn.Fail(
            self,
            "CleanFailed",
            cause="Clean stage failed with non-NOOP exit code.",
            error="CleanStageFailure",
        )
        clean_error_router = sfn.Choice(self, "CleanFailureRouter")
        clean_error_router.when(
            noop_condition("$.clean_error.Cause"),
            clean_noop.next(stage_item_done),
        ).otherwise(clean_fail)
        clean_stage.add_catch(clean_error_router, result_path="$.clean_error")

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

        qaqc_noop = sfn.Pass(
            self,
            "QaqcNoop",
            result=sfn.Result.from_object({"stage": "qaqc", "status": "NOOP"}),
            result_path="$.qaqc_result",
        )
        qaqc_fail = sfn.Fail(
            self,
            "QaqcFailed",
            cause="QAQC stage failed with non-NOOP exit code.",
            error="QaqcStageFailure",
        )
        qaqc_error_router = sfn.Choice(self, "QaqcFailureRouter")
        qaqc_error_router.when(
            noop_condition("$.qaqc_error.Cause"),
            qaqc_noop.next(stage_item_done),
        ).otherwise(qaqc_fail)
        qaqc_stage.add_catch(qaqc_error_router, result_path="$.qaqc_error")

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

        merge_noop = sfn.Pass(
            self,
            "MergeNoop",
            result=sfn.Result.from_object({"stage": "merge", "status": "NOOP"}),
            result_path="$.merge_result",
        )
        merge_fail = sfn.Fail(
            self,
            "MergeFailed",
            cause="Merge stage failed with non-NOOP exit code.",
            error="MergeStageFailure",
        )
        merge_error_router = sfn.Choice(self, "MergeFailureRouter")
        merge_error_router.when(
            noop_condition("$.merge_error.Cause"),
            merge_noop.next(stage_item_done),
        ).otherwise(merge_fail)
        merge_stage.add_catch(merge_error_router, result_path="$.merge_error")

        per_station_chain = (
            sfn.Chain.start(clean_stage)
            .next(qaqc_stage)
            .next(merge_stage)
            .next(stage_item_done)
        )

        process_stations = sfn.Map(
            self,
            "ProcessStations",
            items_path="$.work_items",
            result_path="$.station_results",
            max_concurrency=25,
            item_selector={
                "run_id.$": "$.run_id",
                "network.$": "$$.Map.Item.Value.network",
                "station_id.$": "$$.Map.Item.Value.station_id",
                "last_timestamp.$": "$$.Map.Item.Value.last_timestamp",
            },
        )
        process_stations.item_processor(per_station_chain)

        branch_on_work = sfn.Choice(self, "HasWorkItems")
        branch_on_work.when(
            sfn.Condition.is_present("$.work_items[0]"),
            process_stations,
        ).otherwise(worklist_empty)

        definition = sfn.Chain.start(build_worklist).next(branch_on_work)

        state_machine = sfn.StateMachine(
            self,
            "HdpPhase2StateMachine",
            state_machine_name=run_batch_workflow_name,
            definition_body=sfn.DefinitionBody.from_chainable(definition),
            timeout=Duration.hours(24),
        )

        CfnOutput(self, "StateMachineArn", value=state_machine.state_machine_arn)
