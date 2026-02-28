from aws_cdk import (
    Stack,
    Duration,
    RemovalPolicy,
    aws_s3 as s3,
    aws_lambda as _lambda,
    aws_s3_notifications as s3_notify,
    aws_sqs as sqs, 
    aws_ec2 as ec2,  # Added for VPC Network
    aws_ecs as ecs,  # Added for Fargate Container
)
from constructs import Construct

class GestaltStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # 1. Define the Gestalt Ingestion Bucket
        raw_data_bucket = s3.Bucket(
            self, "GestaltRawDataBucket",
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True 
        )

        # 2. Define the Splitter Queue
        # We increase the visibility timeout because downloading and 
        # splitting a 4GB PDF will take longer than the default 30 seconds.
        splitter_queue = sqs.Queue(
            self, "GestaltSplitterQueue",
            visibility_timeout=Duration.minutes(15) 
        )

        # 3. Define the Gestalt Lambda Router
        ingestion_router = _lambda.Function(
            self, "GestaltIngestionRouter",
            runtime=_lambda.Runtime.PYTHON_3_11,
            handler="ingestion_router.handler", 
            code=_lambda.Code.from_asset("src"), 
            environment={
                "BUCKET_NAME": raw_data_bucket.bucket_name,
                "SPLITTER_QUEUE_URL": splitter_queue.queue_url
            }
        )

        # 4. Grant Permissions
        raw_data_bucket.grant_read(ingestion_router)
        splitter_queue.grant_send_messages(ingestion_router)

        # 5. Wire the Trigger
        raw_data_bucket.add_event_notification(
            s3.EventType.OBJECT_CREATED,
            s3_notify.LambdaDestination(ingestion_router)
        )

        # ==========================================
        # PHASE 2: THE SCRAPER INFRASTRUCTURE (New)
        # ==========================================

        # 1. The Staging Bucket (No triggers attached!)
        staging_bucket = s3.Bucket(
            self, "GestaltStagingBucket",
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True
        )

        # 2. The Network (VPC with 0 NAT Gateways to save money)
        vpc = ec2.Vpc(
            self, "GestaltVpc",
            max_azs=2,
            nat_gateways=0,
            subnet_configuration=[
                ec2.SubnetConfiguration(
                    name="Public",
                    subnet_type=ec2.SubnetType.PUBLIC
                )
            ]
        )

        # 3. The ECS Cluster
        cluster = ecs.Cluster(self, "GestaltCluster", vpc=vpc)

        # 4. The Fargate Task Definition (The "Machine" Blueprint)
        # 2 vCPU and 4GB RAM is a good starting point for a Playwright Chromium browser
        scraper_task = ecs.FargateTaskDefinition(
            self, "GestaltScraperTask",
            cpu=2048,
            memory_limit_mib=4096,
        )

        # Give the machine permission to write to your new Staging Bucket
        # This automatically grants s3:ListBucket, s3:GetObject, and s3:PutObject
        staging_bucket.grant_read_write(scraper_task.task_role)

        # 5. The Container Definition
        # This tells CDK to look in your "scraper" folder, build the Dockerfile it finds there,
        # automatically push it to AWS ECR, and wire it to this Fargate task.
        scraper_task.add_container(
            "ScraperContainer",
            image=ecs.ContainerImage.from_asset("scraper"),
            logging=ecs.LogDrivers.aws_logs(stream_prefix="GestaltScraper"),
            environment={
                "STAGING_BUCKET": staging_bucket.bucket_name
            }
        )