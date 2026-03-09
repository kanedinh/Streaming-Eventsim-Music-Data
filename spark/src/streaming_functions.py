from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, month, hour, dayofmonth, col, year, udf, second

@udf
def string_decode(s, encoding='utf-8'):
    if s:
        return (s.encode('latin1')         # To bytes, required by 'unicode-escape'
                .decode('unicode-escape') # Perform the actual octal-escaping decode
                .encode('latin1')         # 1:1 mapping back to bytes
                .decode(encoding)         # Decode original encoding
                .strip('\"'))

    else:
        return s

def create_or_get_spark_session(app_name: str, master : str="yarn", ram: str="1g", cores: str="1") -> SparkSession:
    """
    Create or get a Spark Session.

    Args:
        app_name (str): Pass the name of your app
        master (str): Choosing the Spark master, default is yarn
        ram (str): Amount of RAM to allocate to each executor, default is 1g
        cores (str): Number of cores to allocate to each executor, default is 1
    Returns:
        spark: SparkSession
    """
    spark = (SparkSession
             .builder
             .appName(app_name)
             .master(master=master)
             .config("spark.executor.memory", ram)
             .config("spark.executor.cores", cores)
             .config("spark.driver.host", "spark-driver")
             .getOrCreate())

    return spark


def create_kafka_read_stream(spark, kafka_address, kafka_port, topic: str, starting_offset="earliest", maxOffset: int=100000):
    """
    Create a kafka read stream

    Args:
        spark (SparkSession): A SparkSession object
        kafka_address (str): Host address of the kafka bootstrap server
        kafka_port (int): Port of the kafka bootstrap server
        topic (str): Name of the kafka topic
        starting_offset (str): Starting offset configuration, "earliest" by default
        maxOffset (int): Max offset to read per trigger, default is 100000
    Returns:
        read_stream: DataStreamReader
    """

    read_stream = (spark
                .readStream
                .format("kafka")
                .option("kafka.bootstrap.servers", f"{kafka_address}:{kafka_port}")
                .option("failOnDataLoss", False)
                .option("startingOffsets", starting_offset)
                .option("subscribe", topic)
                .option("maxOffsetsPerTrigger", maxOffset)
                .load())

    return read_stream


def process_stream(stream, schema, topic):
    """
    Process stream to fetch on value from the kafka message.
    Convert ts to timestamp format and produce year, month, day, hour columns.

    Args:
        stream (DataStreamReader): The data stream reader for your stream
        schema: The schema of the data in the stream
        topic (str): The topic name of the stream
    Returns:
        stream: DataStreamReader
    """

    # read value from the incoming message and convert the contents
    # inside to the passed schema
    stream = (stream
              .select(from_json(col("value").cast("string"), schema).alias("data"))
              .select("data.*")
              )

    # convert ts (UNIX mili-second) to timestamp format (YYYY-MM-DD HH:MM:SS)
    # Add year, month, day to split the data into separate directories
    stream = (stream
              .withColumn("ts", (col("ts")/1000).cast("timestamp"))
              .withColumn("year", year(col("ts")))
              .withColumn("month", month(col("ts")))
              .withColumn("day", dayofmonth(col("ts")))
              )

    # rectify string encoding
    if topic in ["listen_events", "page_view_events"]:
        stream = (stream
                .withColumn("song", string_decode("song"))
                .withColumn("artist", string_decode("artist")) 
                )

    return stream


def create_file_write_stream(stream, storage_path: str, checkpoint_path: str, trigger="60 seconds", output_mode="append", file_format="parquet"):
    """
    Write the stream back to a file store

    Args:
        stream (DataStreamReader): The data stream reader for your stream
        file_format (str): The file format to write to, e.g., parquet, csv, orc etc
        storage_path (str): The file output path
        checkpoint_path (str): The checkpoint location for spark
        trigger (str): The trigger interval
        output_mode (str): The output mode, e.g., append, complete, update
    
    Returns:
        write_stream: DataStreamWriter
    """

    write_stream = (stream
                    .writeStream
                    .format(file_format)
                    .partitionBy("year", "month", "day")
                    .option("path", storage_path)
                    .option("checkpointLocation", checkpoint_path)
                    .trigger(processingTime=trigger)
                    .outputMode(output_mode))

    return write_stream