from airflow import DAG
from airflow.operators.python import PythonOperator
from scripts.data_extraction import _bikeshare_pipeline
from scripts.init_gcp import _create_biglake,_create_bucket,_create_dataset
from datetime import datetime, timedelta
import os

#Enviroment Variables and Config & Data Paths
PROJECT_ID=os.environ['PROJECT_ID']
DATASET_NAME=os.environ['DATASET_NAME']
BUCKET_NAME=os.environ['BUCKET_NAME']
LOCATION=os.environ['LOCATION']
BIGLAKE_NAME=os.environ['BIGLAKE_NAME']
cfg_path = "config/bikeshare_pipeline.cfg"
base_path = "data/bikeshare_pipeline"

#Dag configuration, configured to run every day at 6AM. Timeout 180 minutes
dag = DAG(
    'bikeshare_etl',
    start_date=datetime.now(),
    schedule_interval="0 6 * * *",
    description='ETL Pipeline for bikesharing data extraction',
    dagrun_timeout=timedelta(minutes=180)
)

#Python Callable for Bucket Creation
create_bucket = PythonOperator(
    task_id="create_bucket",
    python_callable=_create_bucket,
    op_kwargs={
    'PROJECT_ID':PROJECT_ID,
    'BUCKET_NAME':BUCKET_NAME,
    'LOCATION':LOCATION},
    provide_context=True,
    dag=dag
)

#Python Callable for DataSet Creation
create_dataset = PythonOperator(
    task_id="create_dataset",
    python_callable=_create_dataset,
    op_kwargs={
    'PROJECT_ID':PROJECT_ID,
    'DATASET_NAME':DATASET_NAME,
    'LOCATION':LOCATION},
    provide_context=True,
    dag=dag
)

#Python Callable for Data Extraction
bikeshare_pipeline = PythonOperator(
    task_id="bikeshare_pipeline",
    python_callable=_bikeshare_pipeline,
    op_kwargs={
    'cfg_path':cfg_path,
    'base_path':base_path,
    'PROJECT_ID':PROJECT_ID,
    'BUCKET_NAME':BUCKET_NAME,
    'LOCATION':LOCATION},
    provide_context=True,
    dag=dag
)

#Python Callable for BigLake external table creation
create_biglake = PythonOperator(
    task_id="create_biglake",
    python_callable=_create_biglake,
    op_kwargs={
    'PROJECT_ID':PROJECT_ID,
    'DATASET_NAME':DATASET_NAME,
    'BIGLAKE_NAME':BIGLAKE_NAME,
    'BUCKET_NAME':BUCKET_NAME,   
    'LOCATION':LOCATION},
    provide_context=True,
    dag=dag
)

#Tasks sequence and dependencies
[create_bucket,create_dataset] >> bikeshare_pipeline >> create_biglake