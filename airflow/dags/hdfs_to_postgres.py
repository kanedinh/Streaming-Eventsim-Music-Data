from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException
import sys
import argparse

def load_hdfs_to_postgres(spark, path: str, pg_table: str, pg_url: str, pg_properties: dict):
    # Read data from HDFS
    try:
        df = spark.read.parquet(f"{path}")
    except AnalysisException as e:
        if "Path does not exist" in str(e):
            print(f"WARN: Cant find data in {pg_table} at {args.date}")
            return None
        else:
            raise e

    # Write data to PostgreSQL
    write = (df.write
                .format("jdbc")
                .option("url", pg_url)
                .option("dbtable", pg_table)
                .option("user", pg_properties["user"])
                .option("password", pg_properties["password"])
                .option("driver", pg_properties["driver"])
                .option("batchsize", pg_properties["batchsize"])
                .mode("append"))

    return write

if __name__ == "__main__":
    # Get date params from airflow (--date 2026-03-07)
    parser = argparse.ArgumentParser()
    parser.add_argument('--date', required=True, help='Date format YYYY-MM-DD')
    args = parser.parse_args()

    # Cut date string
    # Ex: 2026-03-07 -> y = '2026', m = '03', d = '07'
    y, m, d = args.date.split('-')
    # m = '3', d = '7'
    m = str(int(m))
    d = str(int(d))

    # Init spark
    spark = SparkSession.builder.appName(f"HDFS_To_Postgres_{args.date}").master("spark://spark-master:7077").getOrCreate()
    
    # uri data storage
    base_path = "hdfs://namenode:9000/datalake"
    tail_path = f"year={y}/month={m}/day={d}"

    listen_events_path = f"{base_path}/listen_events/{tail_path}"
    auth_events_path = f"{base_path}/auth_events/{tail_path}"
    page_view_events_path = f"{base_path}/page_view_events/{tail_path}"
    status_change_events_path = f"{base_path}/status_change_events/{tail_path}"
    print(listen_events_path)
    # postgres
    pg_url = "jdbc:postgresql://postgres:5432/postgres_db"
    pg_properties = {
        "user": "admin",
        "password": "admin",
        "driver": "org.postgresql.Driver",
        "batchsize": "100000" 
    }
    
    listen_events_writer = load_hdfs_to_postgres(spark, listen_events_path, "listen_events", pg_url, pg_properties)
    auth_events_writer = load_hdfs_to_postgres(spark, auth_events_path, "auth_events", pg_url, pg_properties)
    page_view_events_writer = load_hdfs_to_postgres(spark, page_view_events_path, "page_view_events", pg_url, pg_properties)
    status_change_events_writer = load_hdfs_to_postgres(spark, status_change_events_path, "status_change_events", pg_url, pg_properties)
    
    if listen_events_writer is not None:
        listen_events_writer.save()
    if auth_events_writer is not None:
        auth_events_writer.save()
    if page_view_events_writer is not None:
        page_view_events_writer.save()
    if status_change_events_writer is not None:
        status_change_events_writer.save()

    spark.stop()