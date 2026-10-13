from aws_cdk import Duration, Stack
from aws_cdk import aws_ecs as ecs
from aws_cdk import aws_events as events
from aws_cdk import aws_events_targets as targets
from aws_cdk import aws_iam as iam
from aws_cdk import aws_logs as logs
from aws_cdk import aws_secretsmanager as secretsmanager
from constructs import Construct


class WorkerStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, *, shared,
                 stage_name: str, config: dict, **kwargs):
        super().__init__(scope, construct_id, **kwargs)
        group = logs.LogGroup(self, "WorkerLogs", retention=logs.RetentionDays.ONE_MONTH)
        self.task = ecs.FargateTaskDefinition(self, "WorkerTask",
            cpu=config["worker_cpu"], memory_limit_mib=config["worker_memory_mib"])
        shared.bucket.grant_read_write(self.task.task_role)
        self.worker_security_group = shared.worker_security_group
        secret = lambda arn: secretsmanager.Secret.from_secret_partial_arn(self, arn.rsplit(":", 1)[-1], arn)
        self.task.add_container("Scraper", image=ecs.ContainerImage.from_registry(config["image_uri"]),
            logging=ecs.LogDrivers.aws_logs(stream_prefix="fmkorea", log_group=group),
            environment={"S3_ENDPOINT_URL": f"https://s3.{self.region}.amazonaws.com",
                "S3_BUCKET": shared.bucket.bucket_name,
                "S3_REGION": self.region,
                "S3_PREFIX": f"fmkorea/{stage_name}"},
            secrets={"DATABASE_URL": ecs.Secret.from_secrets_manager(secret(config["database_url_secret_arn"])),
                "S3_ACCESS_KEY_ID": ecs.Secret.from_secrets_manager(secret(config["s3_access_key_secret_arn"])),
                "S3_SECRET_ACCESS_KEY": ecs.Secret.from_secrets_manager(secret(config["s3_secret_key_secret_arn"]))})
        events.Rule(self, "WorkerFailureRule",
            event_pattern=events.EventPattern(source=["aws.ecs"],
                detail_type=["ECS Task State Change"],
                detail={"clusterArn": [shared.cluster.cluster_arn],
                    "lastStatus": ["STOPPED"], "stopCode": ["TaskFailedToStart", "EssentialContainerExited"]}),
            targets=[targets.CloudWatchLogGroup(group)])
