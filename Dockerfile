# This dockerfile uses the latest version of Airflow and ensures that Python is up to date with all the necessary libraries.
FROM apache/airflow:2.10.5
RUN pip install --upgrade pip
COPY requirements.txt .
RUN pip install -r requirements.txt
USER root
RUN apt-get update
RUN apt-get install wget