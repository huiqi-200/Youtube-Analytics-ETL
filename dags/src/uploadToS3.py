import os
import boto3
from dotenv import load_dotenv
import glob
from pathlib import Path
from datetime import datetime

# TODO: variable to pass down from getYoutubeChannel/DAG
now = datetime.now()
formatted_date_time = now.strftime("%Y/%m/%d/")

# set filepath for uploading files; we will preserve the folder hierachy
# script_run_location = Path.cwd()
# os.chdir(script_run_location)

file_pattern_dict = {
    "RawData": "/*.json",
    "ProcessedData": "**/**/**/**/*.csv",
}


def upload_files():

    # set variables needed
    load_dotenv()
    s3_access_key = os.getenv("s3_access_key")
    s3_secret_access_key = os.getenv("s3_secret_access_key")
    s3_bucket_name = os.getenv("s3_bucket_name")
    # Initialize a session using Amazon S3
    s3 = boto3.client(
        "s3",
        aws_access_key_id=s3_access_key,
        aws_secret_access_key=s3_secret_access_key,
    )

    # Define the bucket name and the file details
    bucket_name = s3_bucket_name

    try:

        # Get credentials and create an API client
        s3_bucket_name = os.getenv("s3_bucket_name")
        # Upload the file
        upload_files_to_s3(s3_bucket_name, s3, "RawData")
        upload_files_to_s3(s3_bucket_name, s3, "ProcessedData")

    except Exception as e:
        print(f"Error uploading the file: {e}")


def upload_files_to_s3(s3_bucket_name, s3, data_type="RawData"):
    """
    Input:
    s3_bucket_name: str, name of bucket to upload files into
    s3: botocore.client.S3
    data_type: str, either "RawData" or "ProcessedData",
                define in file_pattern_dict for additional folders in s3 root directory

    Output:
        None
        Files will be uploaded in s3 bucket
    """

    for filename in glob.iglob(
        data_type + file_pattern_dict[data_type], recursive=True
    ):
        # For handling folder creation on S3
        filename = filename.replace("\\", "/")
        to_output_filename = filename.replace(
            f"{data_type}/", f"{data_type}/{formatted_date_time}"
        )
        s3.upload_file(filename, s3_bucket_name, to_output_filename)
        print(
            f"File {filename} uploaded to {s3_bucket_name}/{to_output_filename} successfully."
        )


if __name__ == "__main__":
    upload_files()
