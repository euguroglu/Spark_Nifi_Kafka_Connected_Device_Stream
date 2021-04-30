from airflow import DAG
from airflow.providers.http.sensors.http import HttpSensor
from airflow.sensors.filesystem import FileSensor
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.apache.hive.operators.hive import HiveOperator

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

    open('/home/enes/Applications/BD_Project_1/data/product-views.json', 'wb').write(indata.content)


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

         #Hive operator
         creating_commerce_table = HiveOperator(
            task_id="creating_commerce_table",
            hive_cli_conn_id="hive_conn",
            hql="""
                CREATE EXTERNAL TABLE IF NOT EXISTS commerce(
                    start TIMESTAMP,
                    end TIMESTAMP,
                    source STRING,
                    source_number INTEGER
                    )
                ROW FORMAT DELIMITED
                FIELDS TERMINATED BY ','
                STORED AS TEXTFILE
            """
         )

         #Spark operator
         commerce_processing = SparkSubmitOperator(
            task_id = "commerce_processing",
            application = "/home/enes/Applications/BD_Project_1/nifi_spark_kafka_product_view_platform_v2.py",
            conn_id = "spark_conn",
            verbose = False,
            packages = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1"
         )
