CREATE DATABASE youtube_analytics;
GO

CREATE SCHEMA youtube_analytics
USE youtube_analytics;
GO

IF EXISTS youtube_analytics.channels_dim
CREATE TABLE youtube_analytics.channels_dim (
    channel_id character varying(24) NOT NULL ENCODE lzo,
    channel_title character varying(255) ENCODE lzo,
    channel_description varchar(max) ENCODE lzo,
    PRIMARY KEY (channel_id)
);


CREATE TABLE "youtube_analytics"."channels_fact"(
    channel_id VARCHAR(24) PRIMARY KEY,
    view_count integer ,
    subscriber_count integer ,
    video_count integer ,
    title VARCHAR(255), 
    datetime timestamp without time zone ENCODE az64,
);



CREATE TABLE youtube_analytics.videos_dim (
    video_id character varying(11) NOT NULL ENCODE lzo,
    channel_id VARCHAR(24),
    published_at timestamp without time zone ENCODE az64,
    title VARCHAR(255), 
    PRIMARY KEY (video_id)
) 

CREATE TABLE youtube_analytics.videos_fact (
    video_id character varying(11) NOT NULL ENCODE lzo,
    views_count integer ENCODE az64,
    likes_count integer ENCODE az64,
    comments_count integer ENCODE az64,
    favourite_count integer ENCODE az64,
    datetime timestamp  ENCODE az64,
    PRIMARY KEY (video_id)
);
GO
