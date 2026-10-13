import os
from pathlib import Path

import aws_cdk as cdk
import yaml

from stacks.shared_infra_stack import SharedInfraStack
from stacks.worker_stack import WorkerStack


app = cdk.App()
stage = app.node.try_get_context("stage") or "dev"
if stage not in {"dev", "staging", "prod"}:
    raise ValueError("stage must be dev, staging or prod")
config = yaml.safe_load((Path(__file__).parent / "config" / f"{stage}.yaml").read_text(encoding="utf-8"))
env = cdk.Environment(account=os.environ.get("CDK_DEFAULT_ACCOUNT"),
    region=os.environ.get("CDK_DEFAULT_REGION"))
shared = SharedInfraStack(app, f"FMKorea-{stage}-Shared", stage_name=stage, config=config, env=env)
WorkerStack(app, f"FMKorea-{stage}-Worker", shared=shared, stage_name=stage,
    config=config, env=env)
app.synth()
