import os
import json
import boto3
import fitz  # This is PyMuPDF
import time

# Initialize AWS clients
# (Boto3 will automatically use the credentials injected by your Codespace/Docker)
s3 = boto3.client('s3')
sqs = boto3.client('sqs')

# You will need to pass this environment variable when running the container
QUEUE_URL = os.environ.get('SPLITTER_QUEUE_URL')

def process_pdf(download_path):
    """Opens the PDF and separates native text from embedded images."""
    print(f"Opening {download_path} with PyMuPDF...")
    
    doc = fitz.open(download_path)
    
    for page_num in range(len(doc)):
        page = doc[page_num]
        
        # 1. Extract Text
        text = page.get_text()
        if text.strip():
            print(f"  - Page {page_num}: Found {len(text)} characters of native text.")
            # Future step: Send this text to your Database/Embeddings model
            
        # 2. Extract Images (Scans or Photos)
        image_list = page.get_images(full=True)
        if image_list:
            print(f"  - Page {page_num}: Found {len(image_list)} embedded images.")
            for img_index, img in enumerate(image_list):
                xref = img[0]
                base_image = doc.extract_image(xref)
                image_bytes = base_image["image"]
                image_ext = base_image["ext"]
                
                # Future step: Upload these raw image bytes back to S3 
                # and trigger the OCR or Vision queues
                print(f"    * Extracted image {img_index} (Type: {image_ext}, Size: {len(image_bytes)} bytes)")

    doc.close()

def poll_queue():
    """Continuously polls the SQS queue for new files to process."""
    print(f"Listening to queue: {QUEUE_URL}")
    
    while True:
        # Long polling: Wait up to 20 seconds for a message to arrive
        response = sqs.receive_message(
            QueueUrl=QUEUE_URL,
            MaxNumberOfMessages=1,
            WaitTimeSeconds=20 
        )

        if 'Messages' in response:
            for message in response['Messages']:
                receipt_handle = message['ReceiptHandle']
                body = json.loads(message['Body'])
                
                bucket = body['bucket']
                key = body['key']
                
                local_file_path = f"/tmp/{key.replace('/', '_')}"
                
                try:
                    print(f"\nDownloading '{key}' from '{bucket}'...")
                    s3.download_file(bucket, key, local_file_path)
                    
                    # Process the file
                    process_pdf(local_file_path)
                    
                    # Clean up the local file to prevent disk exhaustion
                    os.remove(local_file_path)
                    
                    # Delete the message from the queue so it isn't processed again
                    print(f"Processing complete. Deleting message from SQS.")
                    sqs.delete_message(
                        QueueUrl=QUEUE_URL,
                        ReceiptHandle=receipt_handle
                    )
                    
                except Exception as e:
                    print(f"Error processing {key}: {str(e)}")
                    # If we don't delete the message, SQS's visibility timeout will 
                    # expire and the message will automatically pop back onto the queue for a retry.
        else:
            # No messages, loop continues
            pass

if __name__ == "__main__":
    if not QUEUE_URL:
        print("ERROR: SPLITTER_QUEUE_URL environment variable is not set.")
        exit(1)
        
    poll_queue()