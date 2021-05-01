from airflow import DAG
from airflow.providers.http.sensors.http import HttpSensor
from airflow.sensors.filesystem import FileSensor
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.apache.hive.operators.hive import HiveOperator
from airflow.providers.apache.sqoop.operators.sqoop import SqoopOperator
from airflow.operators.email import EmailOperator

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

         #Spark operator
         commerce_processing = SparkSubmitOperator(
            task_id = "commerce_processing",
            application = "/home/enes/Applications/BD_Project_1/nifi_spark_kafka_product_view_platform_v2.py",
            conn_id = "spark_conn",
            verbose = False,
            packages = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1"
         )

         #Python operator example delay
         delay_python_task = PythonOperator(
            task_id = "delay_python_task",
            python_callable = lambda: time.sleep(300)
        )

         #Hive operator
         creating_commerce_table = HiveOperator(
            task_id="creating_commerce_table",
            hive_cli_conn_id="hive_conn",
            hql="""
                CREATE EXTERNAL TABLE IF NOT EXISTS commerce(
                    source STRING,
                    source_number BIGINT
                    )
                ROW FORMAT DELIMITED
                FIELDS TERMINATED BY ','
                STORED AS PARQUET
                LOCATION '/tmp/data/ecommerce';
            """
         )

         #Sqoop Operator
         hive_to_mysql = SqoopOperator(
            task_id = "hive_to_mysql",
            conn_id = "sqoop_conn",
            cmd_type = "export",
            table = "commerce",
            hcatalog_table = "commerce"
         )

         #Email operator
         send_email_notification = EmailOperator(
            task_id = "send_email_notification",
            to = "airflow_course@yopmail.com",
            subject = "commerce_data_pipeline",
            html_content = "<h3>commerce_data_pipeline</h3>"
         )

         #Define dependencies
         is_connection_available >> downloading_rates >> commerce_processing
         commerce_processing >> creating_commerce_table >> hive_to_mysql >> send_email_notification
