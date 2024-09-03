import re

from datetime import datetime, date
import pyspark.sql.functions as F

"""Transformations for video statistics fact dataframe
    1. Turn *Count columns into Integer type
    2. Add column "extraction_datetime to mark pipeline run time
"""


def turn_columns_to_req_type(df, keyword: str, type: str):
    """
    Takes a dataframe, keyword and transforms columns containing that keyword into 
    the type requested

    For handling issue: numerical strings wont be parsed in with spark.read unless type 
    was set to StringType()
    """
    for column in df.columns:
        if keyword in column:
            df = df.withColumn(column, F.col(column).cast(type))

    return df


def extract_datetime_from_filename(filename, datetime_format: str):
    """Use regex to extract the datetime string
    If there is a valid datetime, return datetime, else return None
    """

    # TODO: make this regex suitable for all diff datetime types
    match = re.search(r"(\d{14})", filename)
    if match:
        datetime_str = match.group(1)
        # check datetime string is valid, else raise Exception in next line
        datetime_obj = datetime.strptime(datetime_str, datetime_format)
        return datetime_str
        print("Extracted datetime:", datetime_obj)
    else:
        print("No datetime found in the filename.")


# def get_extraction_datetime_from_filename(df):
def get_extracted_datetime(df, filename: str, dt_format: str, pyspark_datetime_format: str):
    """
    Takes a dataframe, and datetime_str from filename
    return df
    """
    datetime_str = extract_datetime_from_filename(filename, dt_format)
    df = (
        df.withColumn("datetime_str", F.lit(datetime_str))
        .withColumn("datetime", F.to_timestamp("datetime_str", pyspark_datetime_format))
        .drop("datetime_str")
    )
    return df