from airflow import DAG
from airflow.providers.http.sensors.http import HttpSensor
from airflow.sensors.filesystem import FileSensor
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

from datetime import datetime, timedelta
import requests
import json

default_args = {
    "owner": "airflow",
    "email_on_failure": False,
    "email_on_retry": False,
    "email": "admin@localhost.com",
    "retry": 1,
    "retry_delay": timedelta(minutes=5)
}

#Function to download file and create json file
def download_rates():
    BASE_URL = "https://raw.githubusercontent.com/euguroglu/Spark_Nifi_Kafka_Connected_Device_Stream/master/data/product-views.json"

    indata = requests.get(f"{BASE_URL}")
    with open('indata') as f:
        indata= json.loads("[" +
            f.read().replace("}\n{", "},\n{") +
        "]")

    with open('/home/enes/Applications/BD_Project_1/data/product-views.json', 'a') as outfile:
        json.dump(indata, outfile)
        outfile.write('\n')

with DAG("ecommerce_platform",start_date=datetime(2021, 1, 1),
         schedule_interval="@daily", default_args=default_args, catchup=False) as dag:

         #HTTP sensor example
         is_connection_available = HttpSensor(
            task_id = "is_connection_available",
            http_conn_id = "data_api",
            endpoint = "euguroglu/Spark_Nifi_Kafka_Connected_Device_Stream/master/data/product-views.json",
            response_check = lambda response: "event" in response.text,
            poke_interval = 5,
            timeout = 20
         )

         #Python operator example
         downloading_rates = PythonOperator(
            task_id = "downloading_rates",
            python_callable = download_rates
         )
