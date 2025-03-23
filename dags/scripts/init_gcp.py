
from google.cloud import storage, bigquery

#Function to create bucket over GCS only if the bucket does not exist already.
def _create_bucket(BUCKET_NAME,PROJECT_ID,LOCATION,**kwargs):
    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)
    bucket.project = PROJECT_ID
    bucket.location = LOCATION
    if not bucket.exists():
        bucket.create()

#Function to create a dataset on BigQuery only if the dataset does not already exist.
def _create_dataset(DATASET_NAME,PROJECT_ID,LOCATION,**kwargs):
    client = bigquery.Client()
    dataset_id = f'{PROJECT_ID}.{DATASET_NAME}' 
    dataset = bigquery.Dataset(dataset_id)
    dataset.location = LOCATION
    client.create_dataset(dataset, exists_ok=True)

#Function to create or replace BigLake external table on BigQuery. Using 'dt' and 'hr' as partitions.
def _create_biglake(BUCKET_NAME,DATASET_NAME,PROJECT_ID,LOCATION,BIGLAKE_NAME,**kwargs):
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