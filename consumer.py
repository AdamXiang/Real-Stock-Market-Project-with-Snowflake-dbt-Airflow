"""
Kafka to S3/MinIO Bronze Layer Ingestion pipeline.

This module acts as a streaming consumer that reads real-time stock quote
payloads from a Kafka topic and sinks them into a local MinIO bucket as
raw JSON files. This represents the ingestion phase of a Medallion architecture.
"""

import os
import json
import time
import boto3
from botocore.client import Config
from botocore.exceptions import ClientError
from kafka import KafkaConsumer
from dotenv import load_dotenv

# Load environment variables securely
load_dotenv(verbose=True)


def initialize_s3_client() -> boto3.client:
    """
    Initializes and configures the Boto3 S3 client for MinIO compatibility.

    Uses path-style addressing which is mandatory for local MinIO setups,
    preventing DNS resolution errors associated with AWS virtual-hosted style.

    Returns:
        boto3.client: A configured Boto3 client connected to the local MinIO instance.
    """
    return boto3.client(
        's3',
        endpoint_url='http://localhost:9002',
        aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
        config=Config(s3={'addressing_style': 'path'})
    )


def ensure_bucket_exists(s3_client: boto3.client, bucket_name: str) -> None:
    """
    Validates the existence of the target S3 bucket and creates it if absent.

    This acts as an idempotent setup step. It is crucial to execute this
    function only once during application startup rather than per-message
    to prevent severe I/O bottlenecks.

    Args:
        s3_client (boto3.client): The active S3/MinIO client.
        bucket_name (str): The name of the target bucket.
    """
    try:
        s3_client.head_bucket(Bucket=bucket_name)
        print(f"Validated existing bucket: '{bucket_name}'.")
    except ClientError:
        print(f"Bucket '{bucket_name}' not found. Creating bucket...")
        s3_client.create_bucket(Bucket=bucket_name)


def sink_record_to_s3(s3_client: boto3.client, bucket_name: str, record: dict) -> None:
    """
    Transforms the Kafka payload into a JSON object and sinks it to S3.

    Constructs a partition-like S3 key using the stock symbol and the timestamp
    to ensure uniqueness and logical grouping.

    Args:
        s3_client (boto3.client): The active S3/MinIO client.
        bucket_name (str): The target bucket for the bronze layer.
        record (dict): The deserialized JSON dictionary retrieved from Kafka.
    """
    # Extract metadata for routing/partitioning; fallback to current time if missing
    symbol = record.get('symbol', 'UNKNOWN')
    ts = record.get('fetched_at', int(time.time()))

    # Construct S3 key (e.g., AAPL/1678886400.json)
    key = f'{symbol}/{ts}.json'

    # Serialize dictionary back to JSON string and upload
    s3_client.put_object(
        Bucket=bucket_name,
        Key=key,
        Body=json.dumps(record),
        ContentType='application/json'
    )

    print(f"Successfully saved {symbol} to s3://{bucket_name}/{key}")


def main():
    """
    Main execution entry point for the Kafka consumer process.

    Initializes dependencies, secures the destination bucket, establishes
    the Kafka consumer connection, and loops indefinitely to process the stream.
    """
    bucket_name = 'bronze-stocks-quotes'
    s3 = initialize_s3_client()

    # 💡 Engineering Optimization: Run bucket validation ONCE at startup.
    # Doing this inside the message loop would cause massive network latency overhead.
    ensure_bucket_exists(s3, bucket_name)

    # Initialize the Kafka consumer
    # The value_deserializer automatically converts the incoming byte payload back into a Python dict
    consumer = KafkaConsumer(
        'stock-quotes',
        bootstrap_servers=['localhost:29092'],
        enable_auto_commit=True,
        auto_offset_reset='earliest',
        group_id='bronze-consumer',
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )

    print('Consumer stream started. Listening for messages...')

    try:
        # Blocking loop: Continuously polls the broker for new messages
        for message in consumer:
            record = message.value
            print(f"Received payload: {record}")

            # Delegate the sinking logic to a dedicated function
            sink_record_to_s3(s3, bucket_name, record)

    except KeyboardInterrupt:
        print("\nConsumer gracefully shutting down...")
    finally:
        # Securely close the consumer connection to commit final offsets
        consumer.close()
        print("Kafka connection closed.")


# Execute the script if called directly
if __name__ == "__main__":
    main()