from aws_cdk import Duration, RemovalPolicy, Stack
from aws_cdk import aws_ec2 as ec2
from aws_cdk import aws_ecs as ecs
from aws_cdk import aws_rds as rds
from aws_cdk import aws_s3 as s3
from constructs import Construct


class SharedInfraStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, *, stage_name: str,
                 config: dict, **kwargs):
        super().__init__(scope, construct_id, **kwargs)
        self.vpc = ec2.Vpc(self, "Vpc", ip_addresses=ec2.IpAddresses.cidr(config["vpc_cidr"]),
            max_azs=2, nat_gateways=1)
        self.bucket = s3.Bucket(self, "OutputBucket", versioned=True,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            encryption=s3.BucketEncryption.S3_MANAGED,
            removal_policy=RemovalPolicy.RETAIN)
        self.cluster = ecs.Cluster(self, "WorkerCluster", vpc=self.vpc)
        self.database = rds.DatabaseInstance(self, "JobDatabase",
            engine=rds.DatabaseInstanceEngine.postgres(version=rds.PostgresEngineVersion.VER_16_4),
            instance_type=ec2.InstanceType(config["db_instance_type"]),
            vpc=self.vpc,
            vpc_subnets=ec2.SubnetSelection(subnet_type=ec2.SubnetType.PRIVATE_WITH_EGRESS),
            allocated_storage=20, max_allocated_storage=100,
            storage_encrypted=True,
            backup_retention=Duration.days(7),
            deletion_protection=stage_name == "prod",
            removal_policy=RemovalPolicy.RETAIN)
        self.worker_security_group = ec2.SecurityGroup(self, "WorkerSecurityGroup", vpc=self.vpc)
        self.database.connections.allow_default_port_from(self.worker_security_group)
