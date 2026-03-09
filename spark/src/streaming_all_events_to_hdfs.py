# Run this scripts:
# docker exec spark-driver /opt/spark/bin/spark-submit streaming_all_events_to_hdfs.py

import os
from schema import schema
from streaming_functions import *

# Kafka Topic Names
AUTH_EVENTS_TOPIC = "auth_events"
PAGE_VIEW_EVENTS_TOPIC = "page_view_events"
STATUS_CHANGE_EVENTS_TOPIC = "status_change_events"
LISTEN_EVENTS_TOPIC = "listen_events"

# Kafka address and port
KAFKA_ADDRESS = "kafka"
KAFKA_PORT = 29092

# HDFS output and checkpoint paths
HDFS_OUTPUT_PATH = "hdfs://namenode:9000/datalake"
CHECKPOINT_BASE_PATH = "hdfs://namenode:9000/checkpoint"

# Initialize a spark session
spark = create_or_get_spark_session(app_name='Streaming', master='spark://spark-master:7077', ram='2g', cores='2')
spark.streams.resetTerminated()

# Create and process the streams for each topic
# auth_events
auth_events = create_kafka_read_stream(spark, KAFKA_ADDRESS, KAFKA_PORT, AUTH_EVENTS_TOPIC)
auth_events = process_stream(auth_events, schema[AUTH_EVENTS_TOPIC], AUTH_EVENTS_TOPIC)

# listen_events
listen_events = create_kafka_read_stream(spark, KAFKA_ADDRESS, KAFKA_PORT, LISTEN_EVENTS_TOPIC)
listen_events = process_stream(listen_events, schema[LISTEN_EVENTS_TOPIC], LISTEN_EVENTS_TOPIC)

# page_view_events
page_view_events = create_kafka_read_stream(spark, KAFKA_ADDRESS, KAFKA_PORT, PAGE_VIEW_EVENTS_TOPIC)
page_view_events = process_stream(page_view_events, schema[PAGE_VIEW_EVENTS_TOPIC], PAGE_VIEW_EVENTS_TOPIC)

# status_change_events
status_change_events = create_kafka_read_stream(spark, KAFKA_ADDRESS, KAFKA_PORT, STATUS_CHANGE_EVENTS_TOPIC)
status_change_events = process_stream(status_change_events, schema[STATUS_CHANGE_EVENTS_TOPIC], STATUS_CHANGE_EVENTS_TOPIC)


# Write a file to storage every xx seconds in parquet format
auth_events_writer = create_file_write_stream(auth_events,
                                              f"{HDFS_OUTPUT_PATH}/{AUTH_EVENTS_TOPIC}",
                                              f"{CHECKPOINT_BASE_PATH}/{AUTH_EVENTS_TOPIC}"
                                              )

listen_events_writer = create_file_write_stream(listen_events,
                                                f"{HDFS_OUTPUT_PATH}/{LISTEN_EVENTS_TOPIC}",
                                                f"{CHECKPOINT_BASE_PATH}/{LISTEN_EVENTS_TOPIC}"
                                                )

page_view_events_writer = create_file_write_stream(page_view_events,
                                                   f"{HDFS_OUTPUT_PATH}/{PAGE_VIEW_EVENTS_TOPIC}",
                                                   f"{CHECKPOINT_BASE_PATH}/{PAGE_VIEW_EVENTS_TOPIC}"
                                                   )

status_change_events_writer = create_file_write_stream(status_change_events,
                                                       f"{HDFS_OUTPUT_PATH}/{STATUS_CHANGE_EVENTS_TOPIC}",
                                                       f"{CHECKPOINT_BASE_PATH}/{STATUS_CHANGE_EVENTS_TOPIC}"
                                                       )

auth_events_writer.start()
listen_events_writer.start()
page_view_events_writer.start()
status_change_events_writer.start()

spark.streams.awaitAnyTermination()
