#!/usr/bin/env python3
import os

import aws_cdk as cdk
from stacks.compute_stack import HdpComputeStack
from stacks.orchestrator_stack import HdpOrchestratorStack
from stacks.state_stack import HdpStateStack

app = cdk.App()

account = os.environ.get("CDK_ACCOUNT", "390197508439")
region = os.environ.get("CDK_REGION", "us-west-2")

env = cdk.Environment(account=account, region=region)

state_stack = HdpStateStack(app, "HdpStateStack", env=env)
compute_stack = HdpComputeStack(app, "HdpComputeStack", env=env)
compute_stack.add_dependency(state_stack)
orchestrator_stack = HdpOrchestratorStack(app, "HdpOrchestratorStack", env=env)
orchestrator_stack.add_dependency(state_stack)
orchestrator_stack.add_dependency(compute_stack)

app.synth()
