import json
import urllib.parse
import os
import boto3

sqs = boto3.client('sqs')
SPLITTER_QUEUE_URL = os.environ.get('SPLITTER_QUEUE_URL')

def handler(event, context):
    print("Incoming S3 Event detected!")
    
    for record in event.get('Records', []):
        bucket_name = record['s3']['bucket']['name']
        file_key = urllib.parse.unquote_plus(record['s3']['object']['key'])
        
        message_body = json.dumps({
            "bucket": bucket_name,
            "key": file_key
        })

        # Everything goes to the Splitter Queue now
        try:
            print(f"Routing '{file_key}' to the Splitter Queue...")
            sqs.send_message(
                QueueUrl=SPLITTER_QUEUE_URL,
                MessageBody=message_body
            )
        except Exception as e:
            print(f"Error routing file {file_key}: {str(e)}")
            raise e

    return {
        'statusCode': 200,
        'body': json.dumps('Routed to Splitter.')
    }