from google.cloud import bigquery,storage
from datetime import datetime, timedelta
import pandas as pd
import os, re

PROJECT_ID=os.environ['PROJECT_ID']
BUCKET_NAME=os.environ['BUCKET_NAME']
LOCATION=os.environ['LOCATION']
cfg_path = "config/bikeshare_pipeline.cfg"
base_path = "data/bikeshare_pipeline"

def _get_date():
    pattern = r"EXTRACT_DATE=(\d{4}-\d{2}-\d{2})"
    with open(cfg_path, "r") as file:
        for line in file:
            match = re.search(pattern, line)
            if match:
                extract_date = datetime.strptime(match.group(1), '%Y-%m-%d').strftime("%Y-%m-%d")
                break 
            else:
                extract_date=(datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    return extract_date

def _extract_data(dt):
    client = bigquery.Client()
    if str(dt) == '9999-01-01':
        query = f"""
        SELECT * FROM `bigquery-public-data.austin_bikeshare.bikeshare_trips`
        """
    else:
        query = f"""
        SELECT * FROM `bigquery-public-data.austin_bikeshare.bikeshare_trips`
        WHERE DATE(start_time) = '{dt}'
        """
    return client.query(query).to_dataframe()

def _create_parquet(df):
    df["start_time"] = pd.to_datetime(df["start_time"]) 
    df["dt"] = df["start_time"].dt.strftime("%Y-%m-%d")
    df["hr"] = df["start_time"].dt.strftime("%H")

    for (dt, hr), df_group in df.groupby(["dt", "hr"]):
        dir_path = f"{base_path}/dt={dt}/hr={hr}"
        os.makedirs(dir_path, exist_ok=True)
        file_path = f"{dir_path}/data.parquet"
        df_group=df_group.drop(columns=['dt', 'hr'])
        df_group.to_parquet(
            file_path,
            engine="pyarrow",
            use_deprecated_int96_timestamps=True,
            index=False)
        
def _upload_gcs():
    storage_client = storage.Client()
    bucket = storage_client.bucket(BUCKET_NAME)
    bucket.project = PROJECT_ID
    bucket.location = LOCATION

    for root, _, files in os.walk(base_path):
        for file in files:
            file_path = os.path.join(root, file)
            gcs_file_path = file_path.replace(base_path, "bikeshare")
            blob = bucket.blob(gcs_file_path)
            blob.upload_from_filename(file_path)

def _bikeshare_pipeline():
    dt=_get_date()
    df=_extract_data(dt)
    _create_parquet(df)
    _upload_gcs()
    

    
