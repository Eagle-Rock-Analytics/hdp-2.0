from pathlib import Path

from aws_cdk import (
    CfnOutput,
    Duration,
    RemovalPolicy,
    Stack,
)
from aws_cdk import aws_dynamodb as dynamodb
from aws_cdk import aws_iam as iam
from aws_cdk import aws_lambda as lambda_
from constructs import Construct


class HdpStateStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        watermark_table = dynamodb.Table(
            self,
            "HdpWatermarksTable",
            table_name="hdp-watermarks",
            partition_key=dynamodb.Attribute(
                name="station_id", type=dynamodb.AttributeType.STRING
            ),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            point_in_time_recovery_specification=dynamodb.PointInTimeRecoverySpecification(
                point_in_time_recovery_enabled=True
            ),
            removal_policy=RemovalPolicy.RETAIN,
        )

        run_history_table = dynamodb.Table(
            self,
            "HdpRunHistoryTable",
            table_name="hdp-run-history",
            partition_key=dynamodb.Attribute(
                name="run_id", type=dynamodb.AttributeType.STRING
            ),
            sort_key=dynamodb.Attribute(
                name="station_id", type=dynamodb.AttributeType.STRING
            ),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            point_in_time_recovery_specification=dynamodb.PointInTimeRecoverySpecification(
                point_in_time_recovery_enabled=True
            ),
            removal_policy=RemovalPolicy.RETAIN,
            time_to_live_attribute="expires_at",
        )

        lambda_role = iam.Role(
            self,
            "BuildWorklistLambdaRole",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSLambdaBasicExecutionRole"
                )
            ],
        )

        watermark_table.grant_read_data(lambda_role)
        run_history_table.grant_write_data(lambda_role)

        lambda_code_path = str(
            Path(__file__).resolve().parent.parent / "lambda" / "build_worklist"
        )

        build_worklist_lambda = lambda_.Function(
            self,
            "BuildWorklistLambda",
            function_name="hdp-build-worklist",
            runtime=lambda_.Runtime.PYTHON_3_10,
            handler="build_worklist.handler",
            code=lambda_.Code.from_asset(lambda_code_path),
            timeout=Duration.seconds(60),
            role=lambda_role,
            environment={
                "WATERMARK_TABLE": watermark_table.table_name,
                "RUN_HISTORY_TABLE": run_history_table.table_name,
                "FRESHNESS_DAYS": "7",
            },
        )

        summarize_role = iam.Role(
            self,
            "SummarizeRunLambdaRole",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSLambdaBasicExecutionRole"
                )
            ],
        )
        run_history_table.grant_read_data(summarize_role)
        summarize_role.add_to_policy(
            iam.PolicyStatement(
                actions=["sns:Publish"],
                resources=["*"],
            )
        )

        summarize_code_path = str(
            Path(__file__).resolve().parent.parent / "lambda" / "summarize_run"
        )

        summarize_run_lambda = lambda_.Function(
            self,
            "SummarizeRunLambda",
            function_name="hdp-summarize-run",
            runtime=lambda_.Runtime.PYTHON_3_10,
            handler="summarize_run.handler",
            code=lambda_.Code.from_asset(summarize_code_path),
            timeout=Duration.seconds(60),
            role=summarize_role,
            environment={
                "RUN_HISTORY_TABLE": run_history_table.table_name,
                "SNS_TOPIC_ARN": "",
            },
        )

        CfnOutput(self, "WatermarkTableName", value=watermark_table.table_name)
        CfnOutput(self, "RunHistoryTableName", value=run_history_table.table_name)
        CfnOutput(
            self, "BuildWorklistLambdaName", value=build_worklist_lambda.function_name
        )
        CfnOutput(
            self, "SummarizeRunLambdaName", value=summarize_run_lambda.function_name
        )
