import json
import urllib.parse

def handler(event, context):
    """
    This function wakes up every time a new file is uploaded to the S3 bucket.
    """
    print("Incoming S3 Event detected!")
    
    # S3 events can technically batch multiple records, so we loop through them
    for record in event.get('Records', []):
        bucket_name = record['s3']['bucket']['name']
        
        # We unquote the key to handle spaces or special characters in the filename
        file_key = urllib.parse.unquote_plus(record['s3']['object']['key'])
        
        print(f"SUCCESS: File '{file_key}' was just uploaded to bucket '{bucket_name}'")
        
        # Future Step: Check if file_key ends in .pdf, .jpg, or .mp4 
        # and send a message to the appropriate SQS processing queue here.

    return {
        'statusCode': 200,
        'body': json.dumps('File routed successfully.')
    }