from airflow import DAG
from airflow.operators.python import PythonOperator
from scripts.data_extraction import _bikeshare_pipeline
from scripts.init_gcp import _create_biglake,_create_bucket,_create_dataset
from datetime import datetime, timedelta

dag = DAG(
    'bikeshare_etl',
    start_date=datetime.now(),
    schedule_interval="0 6 * * *",
    description='ETL Pipeline for bikeshare analytics',
    dagrun_timeout=timedelta(minutes=600)
)

create_bucket = PythonOperator(
    task_id="create_bucket",
    python_callable=_create_bucket,
    dag=dag
)

create_dataset = PythonOperator(
    task_id="create_dataset",
    python_callable=_create_dataset,
    dag=dag
)

bikeshare_pipeline = PythonOperator(
    task_id="bikeshare_pipeline",
    python_callable=_bikeshare_pipeline,
    dag=dag
)

create_biglake = PythonOperator(
    task_id="create_biglake",
    python_callable=_create_biglake,
    dag=dag
)

[create_bucket,create_dataset] >> bikeshare_pipeline >> create_biglake