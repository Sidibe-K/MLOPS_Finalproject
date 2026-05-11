from airflow.decorators import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import joblib
import io
from sklearn.datasets import load_breast_cancer
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression
import json
import boto3

# Configuration
S3_BUCKET = "s3bucket-st-mlops" 
MODEL_KEY = "models/model.pkl"
TEST_DATA_KEY = "data/test_data.csv"

@dag(
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=['ml_system'],
)
def breast_cancer_training_pipeline():

    @task
    def load_and_split():
        """1. Load dataset & 2. Split into train/test"""
        data = load_breast_cancer()
        df = pd.DataFrame(data.data, columns=data.feature_names)
        df['target'] = data.target
        
        train, test = train_test_split(df, test_size=0.2, random_state=42)
        
        # Save test data to S3 
        s3_hook = S3Hook(aws_conn_id='aws_default')
        test_buffer = io.StringIO()
        test.to_csv(test_buffer, index=False)
        s3_hook.load_string(test_buffer.getvalue(), TEST_DATA_KEY, S3_BUCKET, replace=True)
        
        return train.to_json()
    
    @task
    def train_and_upload(train_json):
        """Train simple model & 4. Save model to S3"""
        train_df = pd.read_json(io.StringIO(train_json))
        X = train_df.drop('target', axis=1)
        y = train_df['target']
        
        model = LogisticRegression(max_iter=5000)
        model.fit(X, y)
        
        # Serialize
        buffer = io.BytesIO()
        joblib.dump(model, buffer)
        buffer.seek(0)
        
        # Upload
        s3_hook = S3Hook(aws_conn_id='aws_default')
        s3_hook.load_bytes(buffer.getvalue(), MODEL_KEY, S3_BUCKET, replace=True)
        
        return f"Model saved to s3://{S3_BUCKET}/{MODEL_KEY}"

    train_and_upload(load_and_split())

breast_cancer_training_pipeline()


# --- Requirement 2: Airflow Queue Population ---
def push_to_sqs():
    sqs = boto3.client('sqs', region_name='us-east-1')
    queue_url = sqs.get_queue_url(QueueName='breast-cancer-inference-queue')['QueueUrl']
    
    # Load the data
    data = load_breast_cancer()
    
    _, X_test, _, _ = train_test_split(
        data.data, 
        data.target, 
        test_size=0.2, 
        random_state=42
    )
    
    # Sends one message per record to SQS
    for i, record in enumerate(X_test[:10]):
        message = {
            "record_id": f"sample_{i+1:03d}",
            "features": record.tolist()
        }
        sqs.send_message(
            QueueUrl=queue_url, 
            MessageBody=json.dumps(message)
        )

with DAG(
    'sqs_population_pipeline',
    start_date=datetime(2026, 5, 8),
     schedule='@once',
    catchup=False
    
) as population_dag:

    task_push = PythonOperator(
        task_id='push_test_records',
        python_callable=push_to_sqs
    )
    

    