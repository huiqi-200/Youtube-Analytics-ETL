SELECT 
    channels_dim.channel_title, channels_fact.video_count, channels_fact.datetime
FROM
    "youtube_analytics"."youtube_analytics"."channels_dim" as channels_dim
LEFT JOIN 
    "youtube_analytics"."youtube_analytics"."channels_fact" as channels_fact
ON  channels_dim.channel_id = channels_fact.channel_id