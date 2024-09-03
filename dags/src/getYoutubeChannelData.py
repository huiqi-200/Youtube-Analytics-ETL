# -*- coding: utf-8 -*-

# Sample Python code for youtube.channels.list
# See instructions for running these code samples locally:
# https://developers.google.com/explorer-help/code-samples#python

# updated instructions for handling depreciated OAuth requests
# instructions from: https://github.com/analyticswithadam/Python/blob/main/Pull_all_Comments_and_Replies_for_YouTube_Playlists.ipynb

import os
import json

from googleapiclient.discovery import build
from dotenv import load_dotenv

from pathlib import Path

from datetime import datetime, timedelta

# Get credentials
load_dotenv()
youtube_api_key = os.getenv("youtube_api_key")

# Set file path for JSON
script_location = Path.cwd()
output_path = f"{script_location}/RawData"

# Set current date time
now = datetime.now()
formatted_date_time = now.strftime("%Y%m%d%H%M%S")

# Variables used
scopes = ["https://www.googleapis.com/auth/youtube.readonly"]

# For defining required parts for youtube resource
youtube_channel_parts = ["snippet", "brandingSettings", "contentDetails", "statistics"]
youtube_video_parts = ["id", "snippet", "contentDetails", "statistics", "topicDetails"]

# Dictionary of Channel name to channel id
channel_names_dict = {
    "straitstimesonline": "UC4p_I9eiRewn2KoU-nawrDg",
    "TheBusinessTimes": "UC0GP1HDhGZTLih7B89z_cTg",
    "zaobaodotsg": "UCrbQxu0YkoVWu2dw5b1MzNg",
    "Tamil_Murasu": "UCs0xZ60FSNxFxHPVFFsXNTA",
    "Berita Harian": "UC_WgSFSkn7112rmJQcHSUIQ",
}


def get_youtube_service(api_key):
    youtube = build("youtube", "v3", developerKey=api_key)
    return youtube


def get_youtube_channel_stats(youtube, channel_id: str, api_key: str):
    """Takes in youtube service, channel id and returns statistics in response"""

    request = youtube.channels().list(
        part=",".join(youtube_channel_parts), id=channel_id
    )
    response = request.execute()
    return response


def get_rfc_date_time_days_ago(days_ago: int = 365):
    # Get the current date and time
    now = datetime.now()

    # Calculate the date and time one year ago
    days_ago = now - timedelta(days=days_ago)

    # Format the date and time in RFC 3339 format
    days_ago_rfc = days_ago.isoformat() + "Z"
    return days_ago_rfc


def get_youtube_channel_videos(
    youtube, channel_id: str, api_key: str, published_after: str, channel_custom_url
):
    """Takes in channel id and returns json saved pages
    """

    youtube = build("youtube", "v3", developerKey=api_key)

    # Initial Request
    count = 1  # count of page
    request = youtube.search().list(
        part="id", channelId=channel_id, maxResults=50, order="date", type="video"
    )
    response = request.execute()
    # Initial list of video ids
    video_ids = [item["id"]["videoId"] for item in response["items"]]

    video_analytics_response = get_video_statistics(youtube, video_ids)
    output_to_json(
        video_analytics_response,
        file_name=f"{channel_custom_url}_video_page_{count}_{formatted_date_time}",
    )
     
    while response:

        # for catching of pagination
        try:
            next_page_token = response["nextPageToken"]
        except KeyError:
            break

        # goes to next page of result set and saves as json
        # Reference: https://stackoverflow.com/questions/21289493/youtube-data-api-3-php-how-to-get-more-than-50-video-from-a-channel
        # Translation to Python: in https://developers.google.com/youtube/v3/docs/search/list pageToken description 
        if next_page_token:
            count += 1
            request = youtube.search().list(
                part="id",
                channelId=channel_id,
                maxResults=50,
                order="date",
                type="video",
                publishedAfter=published_after,
                pageToken=next_page_token,
            )
            response = request.execute()
            video_ids = [item["id"]["videoId"] for item in response["items"]]

            video_analytics_response = get_video_statistics(youtube, video_ids)
            output_to_json(
                video_analytics_response,
                file_name=f"{channel_custom_url}_video_page_{count}_{formatted_date_time}",
            )


def get_video_statistics(youtube, video_ids: list):
    """Takes in a list of video IDs and returns youtube response"""
    request = youtube.videos().list(
        part=",".join(youtube_video_parts), id=",".join(video_ids)
    )
    response = request.execute()
    return response


def output_to_json(response, file_name: str):
    """output to Json"""
    with open(f"{output_path}/{file_name}.json", "w", encoding="utf-8") as data_file:
        json.dump(response, data_file)


def get_youtube_channel_video_data():

    youtube = get_youtube_service(youtube_api_key)
    one_year_ago_date_time = get_rfc_date_time_days_ago(days_ago=365)
    for channel_name, channel_id in channel_names_dict.items():

        # Output youtube Channel resource response as JSON
        channel_stats_response = get_youtube_channel_stats(
            youtube, channel_id=channel_id, api_key=youtube_api_key
        )
        channel_custom_url = channel_stats_response["items"][0]["snippet"][
            "customUrl"
        ].replace("@", "")

        output_to_json(
            channel_stats_response,
            file_name=f"{channel_custom_url}_channel_{formatted_date_time}",
        )

        # Output youtube video resource response as JSON
        get_youtube_channel_videos(
            youtube,
            channel_id=channel_id,
            api_key=youtube_api_key,
            published_after=one_year_ago_date_time,
            channel_custom_url=channel_custom_url,
        )


if __name__ == "__main__":
    get_youtube_channel_video_data()
