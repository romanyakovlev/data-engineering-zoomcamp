from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from settings import (
    RIDE_SCHEMA,
    FHV_RIDE_SCHEMA,
    GREEN_RIDE_SCHEMA,
    CONSUME_TOPIC_RIDES_CSV,
    TOPIC_WINDOWED_VENDOR_ID_COUNT,
    CONSUME_TOPIC_FHV_RIDES_CSV,
    CONSUME_TOPIC_GREEN_RIDES_CSV,
)
import pandas as pd


def read_from_kafka(consume_topic: str):
    # Spark Streaming DataFrame, connect to Kafka topic served at host in bootrap.servers option
    df_stream = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092,broker:29092") \
        .option("subscribe", consume_topic) \
        .option("startingOffsets", "earliest") \
        .option("checkpointLocation", "checkpoint") \
        .load()
    return df_stream


def parse_ride_from_kafka_message(df, schema):
    """ take a Spark Streaming df and parse value col based on <schema>, return streaming df cols in schema """
    assert df.isStreaming is True, "DataFrame doesn't receive streaming data"

    df = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

    # split attributes to nested array in one Column
    col = F.split(df['value'], ', ')

    # expand col to multiple top-level columns
    for idx, field in enumerate(schema):
        df = df.withColumn(field.name, col.getItem(idx).cast(field.dataType))
    return df.select([field.name for field in schema])


def sink_console(df, output_mode: str = 'complete', processing_time: str = '5 seconds'):
    write_query = df.writeStream \
        .outputMode(output_mode) \
        .trigger(processingTime=processing_time) \
        .format("console") \
        .option("truncate", False) \
        .start()
    return write_query  # pyspark.sql.streaming.StreamingQuery



def sink_kafka(df, topic, output_mode):
    write_query = df.writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092,broker:29092") \
        .outputMode(output_mode) \
        .option("topic", topic) \
        .option("checkpointLocation", "checkpoint") \
        .start()
    return write_query


def prepare_df_to_kafka_sink(df, value_columns, key_column=None):
    df = df.withColumn("value", F.concat_ws(', ', *value_columns))
    if key_column:
        df = df.withColumnRenamed(key_column, "key")
        df = df.withColumn("key", df.key.cast('string'))
    return df.select(['key', 'value'])


def op_groupby(df, column_names):
    df_aggregation = df.groupBy(column_names).count()
    return df_aggregation


if __name__ == "__main__":
    spark = SparkSession.builder.appName('streaming-examples').getOrCreate()
    spark.sparkContext.setLogLevel('WARN')

    # fhv
    df_fhv_consume_stream = read_from_kafka(consume_topic=CONSUME_TOPIC_FHV_RIDES_CSV)
    df_fhv_rides = parse_ride_from_kafka_message(df_fhv_consume_stream, FHV_RIDE_SCHEMA)

    # green
    df_green_consume_stream = read_from_kafka(consume_topic=CONSUME_TOPIC_GREEN_RIDES_CSV)
    df_green_rides = parse_ride_from_kafka_message(df_green_consume_stream, GREEN_RIDE_SCHEMA)

    df_rides = df_fhv_rides.union(df_green_rides)
    df_trip_count_by_pu_location_id = op_groupby(df_rides, ['PUlocationID'])
    sink_console(df_trip_count_by_pu_location_id)

    # write the output to the kafka topic
    df_kafka_data = prepare_df_to_kafka_sink(
        df=df_rides,
        value_columns=["DOlocationID", "pickup_datetime", "dropOff_datetime"], key_column='PUlocationID'
    )
    kafka_sink_query = sink_kafka(df=df_kafka_data, topic=CONSUME_TOPIC_RIDES_CSV, output_mode="append")

    spark.streams.awaitAnyTermination()

"""
how to run:
python3 producer.py
python3 consumer.py
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1,org.apache.kafka:kafka-clients:2.8.1 ./streaming.py
"""