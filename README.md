# Airflow Bikeshare

This repository contains an implementation of a data pipeline using Apache Airflow to process bikeshare system data. 
The main goal is to extract trip data, transform it, and load it into a data warehouse for further analysis.

## Project Structure

The project is organized as follows:

- **`config/`**: Contains configuration files required for the data pipeline and service account credentials for GCP connection.  
- **`dags/`**: Includes the python DAG "etl_bikeshare.py" that define the Airflow workflow sequence.  
- **`dags/scripts/`**: Includes python scripts that the define the task used by the DAG.  
- **`data/`**: Temporary directory to handle data files during extraction.
- **`.env`**: Stores environment variables used in the project.  
- **`Dockerfile`**: Defines the Docker image for the Airflow environment in tandem with the package requirements for the scripts.  
- **`docker-compose.yaml`**: Configuration file to orchestrate the necessary Docker services to run Airflow. It uses the default official Airflow file (https://airflow.apache.org/docs/apache-airflow/stable/docker-compose.yaml) plus some modifications to accomodate the project needs.
- **`requirements.txt`**: Lists the Python dependencies required for the project.  

## Files Specific Details

- **`dags/bikeshare_etl.py`**:
- **`dags/scripts/init_gcp.py`**:
- **`dags/scripts/data_extraction.py`**:

## Prerequisites

Before getting started, ensure you have the following installed:

- **Docker**: Tool to create and manage containers.  
- **Docker Compose**: Allows defining and running multi-container applications with Docker.  

## Setup and Installation

1. **Clone the repository**:

   ```bash
   git clone https://github.com/jpbotta-zeals/airflow-bikeshare.git
   cd airflow-bikeshare
   mv .env.example .env
   ```

2. **Set up environment variables**:

   Adjust the variables as needed. Take in consideration naming convention for the different services in GCP.

3. **Set up service account file**:

   Create and replace GCP service account key file located in `config/service_account.json` following official documentation:
   
   https://cloud.google.com/iam/docs/keys-create-delete
   
   Service account must have these roles:
   
- **Bigquery Admin** : Needed to complete tasks as create datasets, tables and query information.
- **Cloud Storage Admin** : Needed to complete tasks as create buckets, and upload files.

## Usage

1. **Start Airflow services**:

   Use Docker Compose to refresh configs and start the required services:

   ```bash
   docker-compose up airflow-init
   docker-compose up
   ```

2. **Access the Airflow web interface**:

   Open your browser and go to `http://localhost:8080` to access the Airflow UI (user:airflow/pass:airflow).

3. **Trigger the DAG**:

   In the Airflow UI, enable and run the DAG "bikeshare_etl" responsible for processing bikeshare data.

## Data Analysis

The following queries respond to required questions using the newley BigLake created table by the Airflow pipeline. Replace query sintaxis with `.env` information as needed:

1. **Find the total number of trips for each day.**

	```sql
    select 
    dt as trip_date, 
    count(trip_id) as cnt_trips 
    from `new_dataset.new_table_name`
    group by dt
    order by count(trip_id) desc
	```

2. **Calculate the average trip duration for each day.**

	```sql
	select 
    dt as date_trip, 
    avg(duration_minutes) as time_trip_avg
    from `new_dataset.new_table_name`
    group by dt
    order by avg(duration_minutes) desc
	```

3. **Identify the top 5 stations with the highest number of trip starts.**

	```sql
    select 
    start_station_id,
    count(trip_id) as cnt_trips
    from `new_dataset.new_table_name`
    group by start_station_id
    order by count(trip_id) desc
    limit 5
	```

4. **Find the average number of trips per hour of the day.**

	```sql
    select 
    hr as hour_trip,
    avg(cnt_trip) avg_cnt_trip, 
    from (
    select count(trip_id) as cnt_trip, dt,hr 
    from `new_dataset.new_table_name`
    group by dt,hr)
    group by hr
    order by avg(cnt_trip) desc
	```

5. **Determine the most common trip route (start station to end station).**

	```sql
    select 
    start_station_id,
    end_station_id,
    count(trip_id) as cnt_trips
    from `new_dataset.new_table_name`
    group by start_station_id,end_station_id
    order by count(trip_id) desc
    limit 1
	```

6. **Calculate the number of trips each month.**

	```sql
	select 
    EXTRACT(MONTH from dt) as month_trip,
    count(trip_id) as cnt_trips
    from `new_dataset.new_table_name`
    group by EXTRACT(MONTH from dt)
    order by count(trip_id) desc
	```

7. **Find the station with the longest average trip duration.**

	```sql
	select 
    start_station_id,
    avg(duration_minutes) as time_trip_avg
    from `new_dataset.new_table_name`
    group by start_station_id
    order by avg(duration_minutes) desc
    limit 1
	```

8. **Find the busiest hour of the day (most trips started).**

	```sql
	select 
    hr as start_hour_trip,
    count(trip_id) as cnt_trips
    from `new_dataset.new_table_name`
    group by hr
    order by count(trip_id) desc
    limit 1
	```

9. **Identify the day with the highest number of trips.**

	```sql
	select 
    dt as start_date_trip,
    count(trip_id) as cnt_trips
    from `new_dataset.new_table_name`
    group by dt
    order by count(trip_id) desc
    limit 1
	```
