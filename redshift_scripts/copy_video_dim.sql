COPY youtube_analytics.videos_dim (video_id, channel_id, published_at, title)
FROM 's3 url path of your channels_fact_folder' 
IAM_ROLE 'arn of your iam role' 
FORMAT AS CSV DELIMITER ',' QUOTE '"' IGNOREHEADER 1 REGION AS 'your region'