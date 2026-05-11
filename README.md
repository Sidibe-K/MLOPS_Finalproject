

### Final Project: Building an Asynchronous AI Inference System

## Components and Setup

### Airflow Training DAG
* **Dataset:** Breast Cancer.
* **Process:** Loads data, splits into Train/Test, and trains a Model.
* **Output:** Serializes the model as `model.pkl` and uploads it to S3.
* **File** The code is located in "training_pipeline.py"

### Airflow Queue Population
The 'sqs_population_pipeline' DAG: 
* Reads the test dataset and formats each row into a JSON message containing record_id and features.
* Pushes messages to an **Amazon SQS** queue.
* **File** The code is located in "training_pipeline.py"

### Kubernetes Consumer
The consumer is a containerized Python application that:
1. Loads `model.pkl` from S3 on startup.
2. Polls SQS for new records.
3. Performs inference and writes a unique JSON result to `s3://[bucket]/predictions/`.
4. Deletes the message from the queue upon successful processing.
5. **File** The logic is located in "consumer.py"


##  Run Instructions

### Containerization

The `Dockerfile` defines the environment for the consumer app. To build and push the image to a registry:
```bash
docker build -t your-username/ml-consumer:v1 .
docker push your-username/ml-consumer:v1

```

###  Kubernetes Deployment 

Update the deployment.yaml with your current AWS session credentials such as access_key, secret_key and session_token and run:
```bash

kubectl apply -f deployment.yaml

```
---

To demonstrate horizontal scaling, use the following command to increase the worker count to 3:
```bash
kubectl scale deployment ml-consumer-deployment --replicas=3

```

---

### output verification 
Results are stored in S3 as individual JSON files:
```JSON
{
  "record_id": "sample_001",
  "prediction": 1,
  "timestamp": "2026-05-10T19:50:00Z"
}

'''






