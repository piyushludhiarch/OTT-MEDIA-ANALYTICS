CREATE DATABASE IF NOT EXISTS ott_media_gold_db;

USE ott_media_gold_db;

-- Dimension: Users
CREATE EXTERNAL TABLE IF NOT EXISTS dim_user (
    user_id INT,
    name STRING,
    email STRING,
    plan_id INT,
    country STRING,
    signup_date DATE
)
STORED AS PARQUET
LOCATION 'hdfs://localhost:9000/ott_media/hive_gold/dim_user';


-- Dimension: Content
CREATE EXTERNAL TABLE IF NOT EXISTS dim_content (
    content_id INT,
    title STRING,
    content_type STRING,
    genre STRING,
    duration_minutes INT,
    release_year INT
)
STORED AS PARQUET
LOCATION 'hdfs://localhost:9000/ott_media/hive_gold/dim_content';


-- Dimension: Device
CREATE EXTERNAL TABLE IF NOT EXISTS dim_device (
    device_key INT,
    device_type STRING
)
STORED AS PARQUET
LOCATION 'hdfs://localhost:9000/ott_media/hive_gold/dim_device';


-- Fact: Watch Events
CREATE EXTERNAL TABLE IF NOT EXISTS fact_watch_events (
    event_id STRING,
    user_id INT,
    content_id INT,
    device_key INT,
    event_type STRING,
    event_time TIMESTAMP,
    watch_seconds INT,
    buffer_seconds INT
)
PARTITIONED BY (
    event_date DATE,
    country STRING
)
STORED AS PARQUET
LOCATION 'hdfs://localhost:9000/ott_media/hive_gold/fact_watch_events';