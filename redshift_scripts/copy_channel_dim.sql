COPY youtube_analytics.channels_dim (channel_id, channel_title, channel_description)
FROM 's3 url path of your channels_fact_folder' 
IAM_ROLE 'arn of your iam role' 
FORMAT AS CSV DELIMITER ',' QUOTE '"' IGNOREHEADER 1 REGION AS 'your region'