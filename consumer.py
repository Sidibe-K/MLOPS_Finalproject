import boto3
import json
import joblib
import io
import os
from datetime import datetime

# Configuration
S3_BUCKET = 's3bucket-st-mlops' 
QUEUE_NAME = 'breast-cancer-inference-queue'

s3 = boto3.client('s3')
sqs = boto3.client('sqs', region_name='us-east-1')

# Load model from S3 on startup
print("Pulling model from S3...")
model_obj = s3.get_object(Bucket=S3_BUCKET, Key='models/model.pkl')
model = joblib.load(io.BytesIO(model_obj['Body'].read()))

def start_consuming():
    q_url = sqs.get_queue_url(QueueName=QUEUE_NAME)['QueueUrl']
    
    while True:
        # Poll SQS
        msgs = sqs.receive_message(QueueUrl=q_url, MaxNumberOfMessages=1, WaitTimeSeconds=5)
        
        if 'Messages' in msgs:
            for m in msgs['Messages']:
                data = json.loads(m['Body'])
                
                # 1. Generate Prediction
                pred = int(model.predict([data['features']])[0])
                
                # 2. Generate timestamp
                current_time = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
                
                # Result output
                result = {
                    "record_id": data['record_id'], 
                    "prediction": pred,
                    "timestamp": current_time  
                }
                
                # Save to S3
                s3.put_object(
                    Bucket=S3_BUCKET,
                    Key=f"predictions/{data['record_id']}.json",
                    Body=json.dumps(result)
                )
                
                # Delete message after processing
                sqs.delete_message(QueueUrl=q_url, ReceiptHandle=m['ReceiptHandle'])
                print(f"Processed {data['record_id']}")

if __name__ == "__main__":
    start_consuming()



