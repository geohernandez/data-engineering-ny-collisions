
#  The following API give you the report of vehicles collision in the New York City (NYC):
#  https://data.cityofnewyork.us/Public-Safety/Motor-Vehicle-Collisions-Crashes/h9gi-nx95
#  Here a Postman call: https://data.cityofnewyork.us/resource/h9gi-nx95.json?crash_date=yyyy-mm-ddT00:00:00.000

import requests
import os
import logging

# Packages for interacting with Azure Blob Storage
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient

import airflow.utils.dates
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator


def _get_nyc_collision_data(year,month,day,output_path):
    api_url = f"https://data.cityofnewyork.us/resource/h9gi-nx95.json?crash_date={year}-{month:0>2}-{day:0>2}T00:00:00.000"
    response = requests.get(api_url)
    filename = os.path.join(output_path,f"nyc_collision_{year}-{month}-{day}.txt")
    if response.status_code == 200:
        with open(filename, "w") as f:
            f.write(response.text)

def _upload_nyc_collision_file_to_blob(container_name,filepath,filename):
    logging.INFO(f"filename is {filename} and filepath is {filepath}")
    blob_client = _get_blob_service_client_sas()
    _upload_blob_file(container_name,filepath,filename)

def _get_blob_service_client_sas():
    account_url = "https://nycdag.blob.core.windows.net/blobnycdata?sp=racwdl&st=2023-04-25T04:53:48Z&se=2025-04-01T12:53:48Z&spr=https&sv=2021-12-02&sr=c&sig=qAoUHBiulLnageE7jVWobGWmVOM%2BPQrfPreDyqucQEs%3D"
    # The SAS token string can be assigned to credential here or appended to the account URL
    # credential = sas_token

    # Create the BlobServiceClient object
    blob_service_client = BlobServiceClient(account_url)
    return blob_service_client

def _upload_blob_file(container_name,blob_service_client,filepath,filename):
    container_client = blob_service_client.get_container_client(container=container_name)
    with open(file=os.path.join(filepath, filename), mode="rb") as data:
        blob_client = container_client.upload_blob(name=filename,data=data, overwrite=True)

dag = DAG(
    dag_id="ny_vehicle_collision",
    start_date=airflow.utils.dates.days_ago(1),
    schedule_interval="@daily",
    template_searchpath="/tmp",
    max_active_runs=1,
)
    
get_nyc_collision_data = PythonOperator(
    task_id="get_ny_collision_data",
    python_callable=_get_nyc_collision_data,
    op_kwargs={
        "year": "{{ execution_date.year }}",
        "month": "{{ execution_date.month }}",
        "day": "{{ execution_date.day}}",
        "output_path": "/tmp/"
    }
)

upload_blob_file = PythonOperator(
    task_id = "upload_blob_file",
    python_callable=_upload_blob_file,
    op_kwargs={
    "container_name":"blobnycdata",
    "filepath":"/tmp/",
    "filename":"nyc_collision_{{yesterday_ds}}.txt"
    }
)


get_nyc_collision_data