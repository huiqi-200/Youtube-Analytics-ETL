with videos_view AS(
SELECT
    dim.channel_id, dim.title, dim.published_at, fact.views_count, fact.datetime as at_date_time,
    RANK() over(partition by dim.channel_id ORDER BY fact.views_count DESC) as ranking
FROM
    "youtube_analytics"."youtube_analytics"."videos_fact" as fact
LEFT JOIN 
    "youtube_analytics"."youtube_analytics"."videos_dim" as dim
ON  fact.video_id = dim.video_id
)
select videos_view.title as video_title, 
        videos_view.published_at,
        videos_view.views_count, 
        channels_dim.channel_title
from videos_view
LEFT JOIN
    "youtube_analytics"."youtube_analytics"."channels_dim" as channels_dim
ON videos_view.channel_id = channels_dim.channel_id
where ranking = 1;