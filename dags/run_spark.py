from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import os

from pyspark.sql import SparkSession

from src.getYoutubeChannelData import get_youtube_channel_video_data
from src.transformYoutubeChannelVideos import transform_youtube_json_files
from src.uploadToS3 import upload_files

from src.test_spark import test_spark_count
# Define default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


def run_transformation():
    # Initialize SparkSession
    spark = SparkSession.builder \
    .appName("HelloWorld") \
    .master("spark://spark-spark-1:7077") \
    .config("spark.executor.memory", "2g") \
    .config("spark.executor.cores", "2") \
    .config("spark.driver.memory", "1g") \
    .getOrCreate()

    transform_youtube_json_files(spark)
    # Stop the SparkSession
    spark.stop()

# Define the DAG
dag = DAG(
    'run_spark',
    default_args=default_args,
    description='test spark works',
    schedule_interval=timedelta(days=1),
)


test_spark = PythonOperator(
    task_id='test_spark_count',
    python_callable=test_spark_count,
    dag=dag
)


# Set task dependencies
test_spark
