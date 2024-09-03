import re
import os
import sys

from pyspark.sql import Row
import pyspark.sql.functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    ArrayType,
    IntegerType,
    BooleanType,
    MapType,
    TimestampType,
)

from pyspark.sql import SparkSession
from pathlib import Path


from src.transformHelperFuncts import get_extracted_datetime, turn_columns_to_req_type
from src.schemas import videos_schema, channel_schema

# Set file path for JSON and seperate youtube video and youtube channel json path into seperate lists
script_location = Path.cwd()
input_folder_path = f"{script_location}/RawData"
output_folder_path = f"{script_location}/ProcessedData"

print(script_location)
files = os.listdir(input_folder_path)

video_file_names = [file for file in files if "video" in file]
channel_file_names = [file for file in files if "channel" in file]


## Datetime format
datetime_format = "%Y%m%d%H%M%S"
pyspark_datetime_format = "yyyyMMddHHmmss"


class YoutubeVideosData:
    def __init__(self, filepath: str, json_schema: StructType, spark):
        self.dataframe = (
            spark.read.option("multiline", "true").schema(json_schema).json(filepath)
        )
        self.filepath = filepath

    def print_data(self):
        self.dataframe.show(30)

    def select_video_dim_df(self):
        """Returns Youtube video dimension as Pyspark Dataframe"""
        select_video_dim_df = self.dataframe.select(
            F.col("items.id").alias("video_id"),
            F.col("items.snippet.channelId").alias("channel_id"),
            F.col("items.snippet.publishedAt").alias("published_at"),
            F.col("items.snippet.title").alias("title"),
            F.col("items.snippet.tags").alias("tags"),
        )
        self.video_dim_df = select_video_dim_df
        return select_video_dim_df

    def get_flattened_video_dim_df(self):
        """Returns Youtube video dimension as Pyspark Dataframe with
        columns: ['video_id', 'channel_id', 'published_at', 'title']
        """
        select_video_dim_df = self.select_video_dim_df()
        video_dim_flatten_df = (
            select_video_dim_df.withColumn(
                "zipped_arrays",
                F.arrays_zip(
                    "video_id",
                    "channel_id",
                    "published_at",
                    "title",
                ),
            )
            .withColumn("zipped_arrays", F.explode("zipped_arrays"))
            .select(
                F.col("zipped_arrays.video_id"),
                F.col("zipped_arrays.channel_id"),
                F.col("zipped_arrays.published_at"),
                F.col("zipped_arrays.title"),
            )
        )

        return video_dim_flatten_df

    def select_video_fact_df(self):
        """
        Select required columns for video fact table
        Returns pyspark.DataFrame
        """
        select_video_fact_df = self.dataframe.select(
            F.col("items.id").alias("video_id"),
            F.col("items.statistics.viewCount").alias("views_count"),
            F.col("items.statistics.likeCount").alias("likes_count"),
            F.col("items.statistics.commentCount").alias("comments_count"),
            F.col("items.statistics.favoriteCount").alias("favourite_count"),
        )
        return select_video_fact_df

    def get_flattened_video_fact_df(self):
        """
        Flattens required columns for video fact table
        Granularity: 1 video's statistics at a given datetime
        """
        select_video_fact_df = self.select_video_fact_df()
        video_fact_flatten_df = (
            select_video_fact_df.withColumn(
                "zipped_arrays",
                F.arrays_zip(
                    "video_id",
                    "views_count",
                    "likes_count",
                    "comments_count",
                    "favourite_count",
                ),
            )
            .withColumn("zipped_arrays", F.explode("zipped_arrays"))
            .select(
                F.col("zipped_arrays.video_id"),
                F.col("zipped_arrays.views_count"),
                F.col("zipped_arrays.likes_count"),
                F.col("zipped_arrays.comments_count"),
                F.col("zipped_arrays.favourite_count"),
            )
        )
        return video_fact_flatten_df

    def transform_fact_df(self, datetime_format: str, pyspark_datetime_format: str):
        """
        Performs the following transformation:
        1. turn all *count columns into integers
        2. Add new column, extracted_datetime to the fact dataframe

        """
        flat_video_fact_df = self.get_flattened_video_fact_df()
        video_fact_converted_type_df = turn_columns_to_req_type(
            flat_video_fact_df, keyword="count", type="integer"
        )

        video_fact_add_datetime_df = get_extracted_datetime(
            video_fact_converted_type_df,
            filename=self.filepath,
            dt_format=datetime_format,
            pyspark_datetime_format=pyspark_datetime_format,
        )
        self.final_fact_df = video_fact_add_datetime_df
        return video_fact_add_datetime_df

    def export_fact_file_to_csv(self, folder_path: str):
        """
        Converts pyspark Dataframe to pyspark Pandas before exporting so that we can use custom filename
        """
        self.final_fact_df.toPandas().to_csv(
            f"{folder_path}".replace("json", "csv")
            .replace("video", "video_fact")
            .replace("ProcessedData/", "ProcessedData/video_fact/"),
            index=False,
        )

    def export_dim_file_to_csv(self, folder_path: str):
        """
        Converts pyspark Dataframe to pyspark Pandas before exporting so that we can use custom filename
        """
        video_dim_df = self.get_flattened_video_dim_df()
        folder_path_edit = (
            f"{folder_path}".replace("json", "csv")
            .replace("video", "video_dim")
            .replace("ProcessedData/", "ProcessedData/video_dim/")
        )
        video_dim_df.toPandas().to_csv(
            folder_path_edit,
            index=False,
        )
        print(f"{folder_path_edit} has been exported")


class YoutubeChannelData:
    def __init__(self, filepath: str, json_schema: StructType, spark):
        self.dataframe = (
            spark.read.option("multiline", "true").schema(json_schema).json(filepath)
        )
        self.filename = os.path.basename(filepath)

    def select_channel_fact_df(self):
        """Returns Youtube channel fact table as Pyspark Dataframe
        1 row will be returned, which will be the statistics, as a list, at the given time JSON was generated
        """
        select_channel_fact_df = self.dataframe.select(
            F.col("items.id").alias("channel_id"),
            F.col("items.statistics.viewCount").alias("view_count"),
            F.col("items.statistics.subscriberCount").alias("subscriber_count"),
            F.col("items.statistics.videoCount").alias("video_count"),
            F.col("items.brandingSettings.channel.title").alias("title"),
        )
        self.channel_fact_df = select_channel_fact_df
        return select_channel_fact_df

    def get_flattened_channel_fact_df(self):
        """Returns Youtube channel fact table as Pyspark Dataframe
        Put the nested list into 1 value = 1 row
        1 row = 1 channel's statistics for a given time
        """
        self.select_channel_fact_df()
        flatten_channel_fact_df = (
            self.channel_fact_df.withColumn(
                "zipped_arrays",
                F.arrays_zip(
                    "channel_id",
                    "view_count",
                    "subscriber_count",
                    "video_count",
                    "title",
                ),
            )
            .withColumn("zipped_arrays", F.explode("zipped_arrays"))
            .select(
                F.col("zipped_arrays.channel_id"),
                F.col("zipped_arrays.view_count"),
                F.col("zipped_arrays.subscriber_count"),
                F.col("zipped_arrays.video_count"),
                F.col("zipped_arrays.title"),
            )
        )
        return flatten_channel_fact_df

    def select_channel_dim(self):
        """
        Select required columns in Youtube Channel response JSON for inserting into
        Data warehouse
        Returns pyspark.Dataframe
        """
        select_channel_dim_df = self.dataframe.select(
            F.col("items.id").alias("channel_id"),
            F.col("items.brandingSettings.channel.title").alias("channel_title"),
            F.col("items.brandingSettings.channel.description").alias("description"),
            F.col("items.brandingSettings.channel.keywords").alias("keywords"),
        )
        return select_channel_dim_df

    def get_flattened_channel_dim_df(self):
        """
        Returns Youtube channel dim table as Pyspark Dataframe
        Put the nested list into 1 value = 1 row
        1 row = 1 channels attributes
        """
        select_channel_dim_df = self.select_channel_dim()
        flatten_channel_dim_df = (
            select_channel_dim_df.withColumn(
                "zipped_arrays",
                F.arrays_zip("channel_id", "channel_title", "description"),
            )
            .withColumn("zipped_arrays", F.explode("zipped_arrays"))
            .select(
                F.col("zipped_arrays.channel_id"),
                F.col("zipped_arrays.channel_title"),
                F.col("zipped_arrays.description"),
            )
        )
        return flatten_channel_dim_df

    def transform_fact_df(self, datetime_format: str, pyspark_datetime_format: str):
        """
        Performs the following transformation:
        1. turn all *count columns into integers
        2. Add new column, extracted_datetime to the fact dataframe

        """
        flat_channel_fact_df = self.get_flattened_channel_fact_df()
        channel_fact_converted_type_df = turn_columns_to_req_type(
            flat_channel_fact_df, keyword="count", type="integer"
        )

        channel_fact_add_datetime_df = get_extracted_datetime(
            channel_fact_converted_type_df,
            filename=self.filename,
            dt_format=datetime_format,
            pyspark_datetime_format=pyspark_datetime_format,
        )
        self.final_fact_df = channel_fact_add_datetime_df
        return channel_fact_add_datetime_df

    def export_fact_file_to_csv(self, folder_path: str):
        """
        Converts pyspark Dataframe to pyspark Pandas before exporting so that we can use custom filename
        """
        self.final_fact_df.toPandas().to_csv(
            f"{folder_path}".replace("json", "csv")
            .replace("channel", "channel_fact")
            .replace("ProcessedData/", "ProcessedData/channel_fact/"),
            index=False,
        )
        print(f"{folder_path} has been transformed and exported")

    def export_dim_file_to_csv(self, folder_path: str):
        """
        Converts pyspark Dataframe to pyspark Pandas before exporting so that we can use custom filename
        """
        channel_dim_df = self.get_flattened_channel_dim_df()
        folder_path_edit = (
            f"{folder_path}".replace("json", "csv")
            .replace("channel", "channel_dim")
            .replace("ProcessedData/", "ProcessedData/channel_dim/")
        )
        channel_dim_df.toPandas().to_csv(
            folder_path_edit,
            index=False,
        )
        print(f"{folder_path} has been transformed and exported")


def transform_youtube_json_files(spark):
    """
    Input:
        spark: Spark session defined
    """

    try:
        for file in video_file_names:
            # Get Video Data Pyspark Datetime

            video_analytics_file_path = f"{input_folder_path}/{file}"
            video_output_folder_path = f"{output_folder_path}/{file}"

            youtube_videos = YoutubeVideosData(
                video_analytics_file_path, json_schema=videos_schema, spark=spark
            )
            transform_fact_df = youtube_videos.transform_fact_df(
                datetime_format=datetime_format,
                pyspark_datetime_format=pyspark_datetime_format,
            )
            youtube_videos.export_dim_file_to_csv(folder_path=video_output_folder_path)
            youtube_videos.export_fact_file_to_csv(folder_path=video_output_folder_path)

        # Get Channel Data pyspark dataframe
        for file in channel_file_names:
            # Get Video Data Pyspark Datetime

            channel_analytics_file_path = f"{input_folder_path}/{file}"
            channel_output_folder_path = f"{output_folder_path}/{file}"

            youtube_channels = YoutubeChannelData(
                channel_analytics_file_path, json_schema=channel_schema, spark=spark
            )
            transform_fact_df = youtube_channels.transform_fact_df(
                datetime_format=datetime_format,
                pyspark_datetime_format=pyspark_datetime_format,
            )
            youtube_channels.export_fact_file_to_csv(
                folder_path=channel_output_folder_path
            )
            youtube_channels.export_dim_file_to_csv(
                folder_path=channel_output_folder_path
            )

    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)  # Exit with a non-zero status code to indicate failure


if __name__ == "__main__":
    spark = SparkSession.builder.master("spark://localhost:7077").getOrCreate()
    transform_youtube_json_files(spark)
