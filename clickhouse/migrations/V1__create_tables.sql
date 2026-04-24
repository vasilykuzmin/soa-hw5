-- Таблица для постоянного хранения
CREATE TABLE IF NOT EXISTS movie_events (
    event_id UUID,
    user_id String,
    movie_id String,
    event_type Enum8(
        'VIEW_STARTED' = 1,
        'VIEW_FINISHED' = 2,
        'VIEW_PAUSED' = 3,
        'VIEW_RESUMED' = 4,
        'LIKED' = 5,
        'SEARCHED' = 6
    ),
    timestamp DateTime64(3, 'UTC'),
    device_type Enum8(
        'MOBILE' = 1,
        'DESKTOP' = 2,
        'TV' = 3,
        'TABLET' = 4
    ),
    session_id String,
    progress_seconds UInt32,
    _inserted_at DateTime DEFAULT now()
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(timestamp)
ORDER BY (user_id, event_type, timestamp);

-- Kafka-движок для чтения топика
CREATE TABLE IF NOT EXISTS movie_events_kafka (
    event_id UUID,
    user_id String,
    movie_id String,
    event_type String,
    timestamp DateTime64(3, 'UTC'),
    device_type String,
    session_id String,
    progress_seconds UInt32
) ENGINE = Kafka()
SETTINGS
    kafka_broker_list = 'kafka:9093',
    kafka_topic_list = 'movie-events',
    kafka_group_name = 'clickhouse_consumer',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1;

-- -- Materialized View для автоматического переноса
CREATE MATERIALIZED VIEW IF NOT EXISTS movie_events_mv TO movie_events AS
SELECT
    event_id,
    user_id,
    movie_id,
    CAST(if(event_type = '', 'VIEW_STARTED', event_type) AS Enum8('VIEW_STARTED' = 1, 'VIEW_FINISHED' = 2, 'VIEW_PAUSED' = 3, 'VIEW_RESUMED' = 4, 'LIKED' = 5, 'SEARCHED' = 6)) AS event_type,
    timestamp,
    CAST(if(device_type = '', 'MOBILE', device_type) AS Enum8('MOBILE' = 1, 'DESKTOP' = 2, 'TV' = 3, 'TABLET' = 4)) AS device_type,
    session_id,
    progress_seconds,
    now() AS _inserted_at
FROM movie_events_kafka
WHERE event_id IS NOT NULL;
