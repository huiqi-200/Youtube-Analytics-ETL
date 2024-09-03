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
    'youtube_data_pipeline',
    default_args=default_args,
    description='A simple YouTube data pipeline',
    schedule_interval=timedelta(days=1),
)

get_youtube_channel_data = PythonOperator(
    task_id='get_youtube_channel_data',
    python_callable=get_youtube_channel_video_data,
    dag=dag
)

transform_youtube_channel_video_data = PythonOperator(
    task_id='transform_youtube_channel_video_data',
    python_callable=run_transformation,
    dag=dag
)

upload_files_to_s3 = PythonOperator(
    task_id='upload_files_to_s3',
    python_callable=upload_files,
    dag=dag
)





# Set task dependencies
get_youtube_channel_data >> transform_youtube_channel_video_data >> upload_files_to_s3