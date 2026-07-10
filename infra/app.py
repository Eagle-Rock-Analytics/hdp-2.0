#!/usr/bin/env python3
import os

import aws_cdk as cdk
from stacks.state_stack import HdpStateStack

app = cdk.App()

account = os.environ.get("CDK_ACCOUNT", "390197508439")
region = os.environ.get("CDK_REGION", "us-west-2")

env = cdk.Environment(account=account, region=region)

HdpStateStack(app, "HdpStateStack", env=env)

app.synth()
