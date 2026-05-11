# Use a lightweight Python image
FROM python:3.9-slim

# Set the working directory inside the container
WORKDIR /app

# Copy the requirements file and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the consumer script into the container
COPY consumer.py .

# Run the consumer script when the container starts
CMD ["python", "consumer.py"]

