from aws_cdk import (
    CfnOutput,
    Stack,
)
from aws_cdk import aws_batch as batch
from aws_cdk import aws_ec2 as ec2
from aws_cdk import aws_iam as iam
from constructs import Construct


class HdpComputeStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        account = Stack.of(self).account
        region = Stack.of(self).region
        image_uri = f"{account}.dkr.ecr.{region}.amazonaws.com/hdp:2.0.2"

        vpc = ec2.Vpc.from_lookup(self, "DefaultVpc", is_default=True)
        try:
            subnet_selection = vpc.select_subnets(
                subnet_type=ec2.SubnetType.PRIVATE_WITH_EGRESS
            )
            subnet_ids = subnet_selection.subnet_ids
        except Exception:
            subnet_ids = [subnet.subnet_id for subnet in vpc.public_subnets]

        batch_sg = ec2.SecurityGroup(
            self,
            "HdpBatchSecurityGroup",
            vpc=vpc,
            allow_all_outbound=True,
            description="Security group for HDP Batch compute environment",
        )

        ecs_instance_role = iam.Role(
            self,
            "HdpBatchEcsInstanceRole",
            assumed_by=iam.ServicePrincipal("ec2.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AmazonEC2ContainerServiceforEC2Role"
                ),
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "AmazonSSMManagedInstanceCore"
                ),
            ],
        )

        ecs_instance_profile = iam.CfnInstanceProfile(
            self,
            "HdpBatchEcsInstanceProfile",
            roles=[ecs_instance_role.role_name],
            instance_profile_name="hdp-batch-ecs-instance-profile",
        )

        batch_service_role = iam.Role(
            self,
            "HdpBatchServiceRole",
            assumed_by=iam.ServicePrincipal("batch.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSBatchServiceRole"
                )
            ],
        )

        task_execution_role = iam.Role(
            self,
            "HdpBatchTaskExecutionRole",
            assumed_by=iam.ServicePrincipal("ecs-tasks.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AmazonECSTaskExecutionRolePolicy"
                )
            ],
        )

        job_role_arn = f"arn:aws:iam::{account}:role/hdp-phase2-batch-job-role"

        compute_env = batch.CfnComputeEnvironment(
            self,
            "HdpBatchComputeEnv",
            compute_environment_name="hdp-phase2-compute",
            type="MANAGED",
            state="ENABLED",
            service_role=batch_service_role.role_arn,
            compute_resources=batch.CfnComputeEnvironment.ComputeResourcesProperty(
                type="SPOT",
                allocation_strategy="SPOT_CAPACITY_OPTIMIZED",
                minv_cpus=0,
                desiredv_cpus=0,
                maxv_cpus=512,
                instance_types=["c7i-flex.large", "m7i-flex.large"],
                subnets=subnet_ids,
                security_group_ids=[batch_sg.security_group_id],
                instance_role=ecs_instance_profile.attr_arn,
            ),
        )
        compute_env.add_dependency(ecs_instance_profile)

        job_queue = batch.CfnJobQueue(
            self,
            "HdpBatchJobQueue",
            job_queue_name="hdp-phase2-queue",
            state="ENABLED",
            priority=10,
            compute_environment_order=[
                batch.CfnJobQueue.ComputeEnvironmentOrderProperty(
                    compute_environment=compute_env.ref,
                    order=1,
                )
            ],
        )

        base_env = [
            batch.CfnJobDefinition.EnvironmentProperty(
                name="HDP_STAGING_BUCKET", value="hdp-staging-pull"
            ),
            batch.CfnJobDefinition.EnvironmentProperty(
                name="HDP_SOURCE_BUCKET", value="auto-hdp"
            ),
            batch.CfnJobDefinition.EnvironmentProperty(
                name="HDP_PUBLISH_BUCKET", value="auto-hdp"
            ),
            batch.CfnJobDefinition.EnvironmentProperty(
                name="HDP_PUBLISH_PREFIX", value="hdp"
            ),
        ]

        def create_job_def(
            job_id: str,
            job_name: str,
            command: list[str],
            memory: int = 4096,
            vcpus: int = 1,
        ) -> batch.CfnJobDefinition:
            return batch.CfnJobDefinition(
                self,
                job_id,
                job_definition_name=job_name,
                type="container",
                platform_capabilities=["EC2"],
                container_properties=batch.CfnJobDefinition.ContainerPropertiesProperty(
                    image=image_uri,
                    command=command,
                    vcpus=vcpus,
                    memory=memory,
                    execution_role_arn=task_execution_role.role_arn,
                    job_role_arn=job_role_arn,
                    environment=base_env,
                    privileged=False,
                ),
                retry_strategy=batch.CfnJobDefinition.RetryStrategyProperty(attempts=1),
                timeout=batch.CfnJobDefinition.TimeoutProperty(
                    attempt_duration_seconds=7200
                ),
            )

        pull_job = create_job_def(
            "HdpPullJobDef",
            "hdp-pull-job",
            ["run-pull", "--dry-run"],
            memory=2048,
        )
        clean_job = create_job_def(
            "HdpCleanJobDef",
            "hdp-clean-job",
            ["run-clean", "--help"],
            memory=4096,
        )
        qaqc_job = create_job_def(
            "HdpQaqcJobDef",
            "hdp-qaqc-job",
            ["run-qaqc", "--help"],
            memory=4096,
        )
        merge_job = create_job_def(
            "HdpMergeJobDef",
            "hdp-merge-job",
            ["run-merge", "--help"],
            memory=4096,
        )
        stationlist_job = create_job_def(
            "HdpStationlistJobDef",
            "hdp-stationlist-job",
            ["python", "--version"],
            memory=4096,
        )

        CfnOutput(self, "BatchComputeEnvironmentName", value=compute_env.ref)
        CfnOutput(self, "BatchJobQueueName", value=job_queue.ref)
        CfnOutput(self, "PullJobDefinitionArn", value=pull_job.ref)
        CfnOutput(self, "CleanJobDefinitionArn", value=clean_job.ref)
        CfnOutput(self, "QaqcJobDefinitionArn", value=qaqc_job.ref)
        CfnOutput(self, "MergeJobDefinitionArn", value=merge_job.ref)
        CfnOutput(self, "StationlistJobDefinitionArn", value=stationlist_job.ref)
