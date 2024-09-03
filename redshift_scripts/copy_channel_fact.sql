COPY youtube_analytics.channels_fact (
    channel_id,
    view_count,
    subscriber_count,
    video_count,
    title,
    datetime
)
FROM 's3 url path of your channels_fact_folder' 
IAM_ROLE 'arn of your iam role' 
FORMAT AS CSV DELIMITER ',' QUOTE '"' IGNOREHEADER 1 REGION AS 'your region'