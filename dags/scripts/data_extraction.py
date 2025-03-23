from google.cloud import bigquery,storage
from datetime import datetime, timedelta
import pandas as pd
import os, re

# Function to obtain extract date.
def _get_date(cfg_path):
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

# Function to extract data from `bikeshare_trips` by query using extract date.
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

# Function to create parquet files from the previously extracted data. 
# Using start_time, extracting date and time, the data is grouped and split into the corresponding temp directories.
def _create_parquet(df,base_path):
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

# Function to upload parquet files into the corresponding bucket directories in GCS.
def _upload_gcs(base_path,BUCKET_NAME,PROJECT_ID,LOCATION):
    storage_client = storage.Client()
    bucket = storage_client.bucket(BUCKET_NAME)
    bucket.project = PROJECT_ID
    bucket.location = LOCATION

    for dirpath, dirnames, filenames in os.walk(base_path):
        for file in filenames:
            file_path = os.path.join(dirpath, file)
            gcs_file_path = file_path.replace(base_path, "bikeshare")
            blob = bucket.blob(gcs_file_path)
            blob.upload_from_filename(file_path)

# Main function for data extraction. 
# Once the data has been uploaded, the temporary directories are emptied.
def _bikeshare_pipeline(BUCKET_NAME,PROJECT_ID,LOCATION,base_path,cfg_path,**kwargs):
    dt=_get_date(cfg_path)
    df=_extract_data(dt)
    _create_parquet(df,base_path)
    _upload_gcs(base_path,BUCKET_NAME,PROJECT_ID,LOCATION)
    os.system(f"rm -rf {base_path}")
    

    
