import os
import boto3
import snowflake.connector
from botocore.client import Config
from airflow.decorators import dag, task
from datetime import datetime, timedelta
from dotenv import load_dotenv

# Load environment variables
# Note: In a production Airflow environment, it is highly recommended to manage
# these credentials using Airflow Connections and Variables instead of a .env file.
load_dotenv(verbose=True)

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
BUCKET = os.getenv("BUCKET")
LOCAL_DIR = os.getenv("LOCAL_DIR")

SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")
SNOWFLAKE_DB = os.getenv("SNOWFLAKE_DB")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA")

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="minio_to_snowflake_taskflow",
    default_args=default_args,
    start_date=datetime(2025, 9, 9),
    schedule_interval="*/1 * * * *",  # Executes every 1 minute
    catchup=False,
    tags=["bronze", "ingestion", "snowflake", "minio"],
    doc_md="""
    # MinIO to Snowflake Ingestion DAG
    This DAG extracts raw JSON stock quotes from a local MinIO bucket and 
    loads them into a Snowflake bronze-layer table via an internal stage.
    """
)
def minio_to_snowflake_etl():
    """
    Defines the TaskFlow execution graph for the MinIO to Snowflake ETL pipeline.
    """

    @task
    def extract_from_minio() -> list:
        """
        Extracts objects from a designated MinIO (S3-compatible) bucket and
        downloads them to the Airflow worker's local filesystem.

        This function initializes an S3 client using path-style addressing
        (crucial for MinIO compatibility), retrieves a list of objects from
        the target bucket, and sequentially downloads them.

        Returns:
            list: A list of absolute local file paths pointing to the downloaded files.
                  This list is automatically serialized and pushed to XCom by TaskFlow.
        """
        os.makedirs(LOCAL_DIR, exist_ok=True)

        # Initialize Boto3 S3 client with path-style addressing for MinIO
        s3 = boto3.client(
            "s3",
            endpoint_url=MINIO_ENDPOINT,
            aws_access_key_id=MINIO_ACCESS_KEY,
            aws_secret_access_key=MINIO_SECRET_KEY,
            config=Config(s3={'addressing_style': 'path'})
        )

        # Retrieve the list of objects currently in the bucket
        objects = s3.list_objects_v2(Bucket=BUCKET).get("Contents", [])
        local_files = []

        for obj in objects:
            key = obj["Key"]
            # Construct the absolute path for the local download destination
            local_file = os.path.join(LOCAL_DIR, os.path.basename(key))

            # Download the file from MinIO to the Airflow worker
            s3.download_file(BUCKET, key, local_file)
            print(f"Successfully downloaded: {key} -> {local_file}")

            local_files.append(local_file)

        return local_files

    @task
    def load_to_snowflake(local_files: list) -> None:
        """
        Ingests locally staged files into a Snowflake table using an internal stage.

        This function iterates through a provided list of local file paths,
        uploads them to a Snowflake table's internal stage using the `PUT` command,
        and subsequently ingests the data into the target table using the `COPY INTO` command.

        Args:
            local_files (list): A list of absolute file paths to be loaded.
                                In TaskFlow, this is automatically pulled from XCom.
        """
        if not local_files:
            print("No new files detected in the staging area. Skipping load operation.")
            return

        # Establish connection to Snowflake using service account credentials
        conn = snowflake.connector.connect(
            user=SNOWFLAKE_USER,
            password=SNOWFLAKE_PASSWORD,
            account=SNOWFLAKE_ACCOUNT,
            warehouse=SNOWFLAKE_WAREHOUSE,
            database=SNOWFLAKE_DB,
            schema=SNOWFLAKE_SCHEMA
        )
        cur = conn.cursor()

        try:
            if not SNOWFLAKE_DB or not SNOWFLAKE_SCHEMA:
                raise ValueError("Snowflake Database or Schema environment variables are missing!")

                # 💡 Force to turn into correct DB and Schema
            cur.execute(f"USE ROLE ACCOUNTADMIN")
            cur.execute(f"USE DATABASE {SNOWFLAKE_DB}")
            cur.execute(f"USE SCHEMA {SNOWFLAKE_SCHEMA}")
            print(f"Session context set to Database: {SNOWFLAKE_DB}, Schema: {SNOWFLAKE_SCHEMA}")

            # Step 1: Stage the files (Upload from local worker to Snowflake internal stage)
            for f in local_files:
                # The @%table_name syntax targets the internal stage tied to the specific table
                put_query = f"PUT file://{f} @%BRONZE_STOCK_QUOTES_RAW auto_compress=true"
                cur.execute(put_query)
                print(f"Staged file to Snowflake: {f}")

            # Step 2: Load the data from the stage into the target table
            # Best practice: log errors but don't fail the entire batch
            copy_query = """
                COPY INTO bronze_stock_quotes_raw
                FROM @%bronze_stock_quotes_raw
                FILE_FORMAT = (TYPE=JSON)
                ON_ERROR = 'CONTINUE'  
            """
            cur.execute(copy_query)
            print("Successfully executed COPY INTO command.")

        except Exception as e:
            print(f"An error occurred during Snowflake ingestion: {e}")
            raise e

        finally:
            # Ensure resources are cleanly closed regardless of success or failure
            cur.close()
            conn.close()

    # --- Define Task Dependencies ---
    # With TaskFlow API, passing the output of one task directly as the input
    # to another automatically establishes the dependency (task1 >> task2)
    # and handles the XCom push/pull under the hood.
    staged_files = extract_from_minio()
    load_to_snowflake(staged_files)


# Instantiate the DAG
minio_to_snowflake_etl()