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

"""Schema for youtube video json"""
statistics_schema = StructType(
    [
        StructField("viewCount", StringType(), True),
        StructField("likeCount", StringType(), True),
        StructField("favoriteCount", StringType(), True),
        StructField("commentCount", StringType(), True),
    ]
)

thumbnails_schema = StructType(
    [
        StructField(
            "default",
            StructType(
                [
                    StructField("url", StringType(), True),
                    StructField("width", IntegerType(), True),
                    StructField("height", IntegerType(), True),
                ]
            ),
            True,
        ),
        StructField(
            "medium",
            StructType(
                [
                    StructField("url", StringType(), True),
                    StructField("width", IntegerType(), True),
                    StructField("height", IntegerType(), True),
                ]
            ),
            True,
        ),
        StructField(
            "high",
            StructType(
                [
                    StructField("url", StringType(), True),
                    StructField("width", IntegerType(), True),
                    StructField("height", IntegerType(), True),
                ]
            ),
            True,
        ),
    ]
)

snippets_schema = StructType(
    [
        StructField("publishedAt", TimestampType(), True),
        StructField("channelId", StringType(), True),
        StructField("title", StringType(), True),
        StructField("description", StringType(), True),
        StructField("thumbnails", thumbnails_schema, True),
        StructField("channelTitle", StringType(), True),
        StructField("tags", ArrayType(StringType()), True),
        StructField("categoryId", StringType(), True),
        StructField("liveBroadcastContent", StringType(), True),
        StructField(
            "localized",
            StructType(
                [
                    StructField("title", StringType(), True),
                    StructField("description", StringType(), True),
                ]
            ),
            True,
        ),
    ]
)
content_details_schema = StructType(
    [
        StructField("duration", StringType(), True),
        StructField("dimension", StringType(), True),
        StructField("definition", StringType(), True),
        StructField("caption", StringType(), True),
        StructField("licensedContent", BooleanType(), True),
        StructField("contentRating", MapType(StringType(), StringType()), True),
        StructField("projection", StringType(), True),
    ]
)
items_schema = StructType(
    [
        StructField("kind", StringType(), True),
        StructField("etag", StringType(), True),
        StructField("id", StringType(), True),
        StructField("snippet", snippets_schema, True),
        StructField("contentDetails", content_details_schema, True),
        StructField("statistics", statistics_schema, True),
    ]
)

videos_schema = StructType(
    [
        StructField("kind", StringType(), True),
        StructField("etag", StringType(), True),
        StructField("items", ArrayType(items_schema), True),
        StructField("pageInfo", MapType(StringType(), StructType([])), True),
    ]
)


channel_snippet_schema = StructType(
    [
        StructField("title", StringType(), True),
        StructField("description", StringType(), True),
        StructField("customUrl", StringType(), True),
        StructField("publishedAt", TimestampType(), True),
        StructField("thumbnails", thumbnails_schema, True),
    ]
)
# Define the schema for contentDetails
channel_content_details_schema = StructType([
    StructField("relatedPlaylists", StructType([
        StructField("likes", StringType(), True),
        StructField("uploads", StringType(), True)
    ]), True)
])

channel_statistics_schema =  StructType(
                [
                    StructField("viewCount", StringType(), True),
                    StructField("subscriberCount", StringType(), True),
                    StructField("hiddenSubscriberCount", BooleanType(), True),
                    StructField("videoCount", StringType(), True),
                ]
            )

# Define the schema for brandingSettings
channel_branding_settings_schema = StructType([
    StructField("channel", StructType([
        StructField("title", StringType(), True),
        StructField("description", StringType(), True),
        StructField("keywords", StringType(), True),
        StructField("unsubscribedTrailer", StringType(), True),
        StructField("defaultLanguage", StringType(), True),
        StructField("country", StringType(), True)
    ]), True),
    StructField("image", StructType([
        StructField("bannerExternalUrl", StringType(), True)
    ]), True)
])

# Define the schema
channel_items_schema = StructType(
    [
        StructField("kind", StringType(), True),
        StructField("etag", StringType(), True),
        StructField("id", StringType(), True),
        StructField("snippet", channel_snippet_schema, True),
        StructField("contentDetails", channel_statistics_schema, True),
        StructField("statistics",channel_statistics_schema,True),
        StructField("brandingSettings",channel_branding_settings_schema, True)
    ]
)

channel_schema = StructType(
    [
        StructField("kind", StringType(), True),
        StructField("etag", StringType(), True),
        StructField("pageInfo", MapType(StringType(), StructType([])), True),
        StructField("items", ArrayType(channel_items_schema), True)
    ]
)