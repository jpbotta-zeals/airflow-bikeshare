
from google.cloud import storage, bigquery
import os

PROJECT_ID=os.environ['PROJECT_ID']
DATASET_NAME=os.environ['DATASET_NAME']
BUCKET_NAME=os.environ['BUCKET_NAME']
LOCATION=os.environ['LOCATION']
BIGLAKE_NAME=os.environ['BIGLAKE_NAME']

def _create_bucket():
    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)
    bucket.project = PROJECT_ID
    bucket.location = LOCATION
    if not bucket.exists():
        bucket.create()

def _create_dataset():
    client = bigquery.Client()
    dataset_id = f'{PROJECT_ID}.{DATASET_NAME}' 
    dataset = bigquery.Dataset(dataset_id)
    dataset.location = LOCATION
    client.create_dataset(dataset, exists_ok=True)

def _create_biglake():
    client = bigquery.Client()
    job_config = bigquery.QueryJobConfig()
    sql = f"""
    CREATE OR REPLACE EXTERNAL TABLE `{PROJECT_ID}.{DATASET_NAME}.{BIGLAKE_NAME}`
    WITH PARTITION COLUMNS
    OPTIONS(
    format="PARQUET",
    hive_partition_uri_prefix="gs://{BUCKET_NAME}/bikeshare/",
    uris=["gs://{BUCKET_NAME}/bikeshare/*/data.parquet"])
    """
    query_job = client.query(
    sql,
    location=LOCATION,
    job_config=job_config)
    query_job.result()  