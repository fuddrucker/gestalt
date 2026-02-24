from aws_cdk import (
    Stack,
    RemovalPolicy,
    aws_s3 as s3,
    aws_lambda as _lambda,
    aws_s3_notifications as s3_notify,
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

        # 2. Define the Gestalt Lambda Router
        ingestion_router = _lambda.Function(
            self, "GestaltIngestionRouter",
            runtime=_lambda.Runtime.PYTHON_3_11,
            handler="ingestion_router.handler", 
            code=_lambda.Code.from_asset("src"), 
            environment={
                "BUCKET_NAME": raw_data_bucket.bucket_name
            }
        )

        # 3. Grant Permissions
        raw_data_bucket.grant_read(ingestion_router)

        # 4. Wire the Trigger
        raw_data_bucket.add_event_notification(
            s3.EventType.OBJECT_CREATED,
            s3_notify.LambdaDestination(ingestion_router)
        )