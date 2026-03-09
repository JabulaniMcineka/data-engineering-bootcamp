import os
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings

# Setup
env = StreamExecutionEnvironment.get_execution_environment()
env.set_parallelism(1)
settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
t_env = StreamTableEnvironment.create(env, settings)

KAFKA_URL = os.environ.get("KAFKA_URL")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC")
KAFKA_WEB_TRAFFIC_KEY = os.environ.get("KAFKA_WEB_TRAFFIC_KEY")
KAFKA_WEB_TRAFFIC_SECRET = os.environ.get("KAFKA_WEB_TRAFFIC_SECRET")
POSTGRES_URL = os.environ.get("POSTGRES_URL")
POSTGRES_USER = os.environ.get("POSTGRES_USER")
POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD")

# Source: Kafka web events
t_env.execute_sql(f"""
    CREATE TABLE web_events (
        ip VARCHAR,
        event_time TIMESTAMP(3),
        host VARCHAR,
        url VARCHAR,
        WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
    ) WITH (
        'connector' = 'kafka',
        'topic' = '{KAFKA_TOPIC}',
        'properties.bootstrap.servers' = '{KAFKA_URL}',
        'properties.group.id' = 'web-events',
        'properties.security.protocol' = 'SASL_SSL',
        'properties.sasl.mechanism' = 'PLAIN',
        'properties.sasl.jaas.config' = 'org.apache.flink.kafka.shaded.org.apache.kafka.common.security.plain.PlainLoginModule required username=\"{KAFKA_WEB_TRAFFIC_KEY}\" password=\"{KAFKA_WEB_TRAFFIC_SECRET}\";',
        'scan.startup.mode' = 'latest-offset',
        'format' = 'json'
    )
""")

# Sessionize by IP and host with 5 minute gap
t_env.execute_sql("""
    CREATE TABLE sessionized_events (
        ip VARCHAR,
        host VARCHAR,
        session_start TIMESTAMP(3),
        session_end TIMESTAMP(3),
        event_count BIGINT
    ) WITH (
        'connector' = 'jdbc',
        'url' = '{POSTGRES_URL}',
        'table-name' = 'sessionized_events',
        'username' = '{POSTGRES_USER}',
        'password' = '{POSTGRES_PASSWORD}'
    )
""".format(
    POSTGRES_URL=POSTGRES_URL,
    POSTGRES_USER=POSTGRES_USER,
    POSTGRES_PASSWORD=POSTGRES_PASSWORD
))

# Session window query with 5 minute gap
t_env.execute_sql("""
    INSERT INTO sessionized_events
    SELECT
        ip,
        host,
        SESSION_START(event_time, INTERVAL '5' MINUTE) AS session_start,
        SESSION_END(event_time, INTERVAL '5' MINUTE) AS session_end,
        COUNT(*) AS event_count
    FROM web_events
    GROUP BY
        ip,
        host,
        SESSION(event_time, INTERVAL '5' MINUTE)
""")