COPY youtube_analytics.videos_fact (video_id, views_count, likes_count, comments_count, favourite_count, datetime)
FROM 's3 url path of your channels_fact_folder' 
IAM_ROLE 'arn of your iam role' 
FORMAT AS CSV DELIMITER ',' QUOTE '"' IGNOREHEADER 1 REGION AS 'your region'