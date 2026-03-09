# Streaming 'Eventsim' Music Data

A data pipeline with Kafka, Spark, Hadoop, dbt, Docker, Airflow and much more!

## Objective

The project will stream events generated from a fake music streaming service (like Spotify) and create a data pipeline that consumes the real-time data. In the end of the day, the batch job will consume the data of that day, apply transformations and create a data warehouse.

## About Data

Eventsim is the program that generates event data from a fake music website. Read more [in here](https://github.com/Interana/eventsim). The docker image is borrowed from [viirya's fork](https://github.com/viirya/eventsim).

Eventsim uses song data from [Million Songs Dataset](http://millionsongdataset.com) to generate events. I have used a [subset](http://millionsongdataset.com/pages/getting-dataset/#subset) of 10000 songs.

## What's Tools & Technologies I use in this Project

- **Data source**: Eventsim.
- **Containerization**: Docker.
- **Stream Processing**: Kafka, Spark Streaming.
- **Data Storage**: Hadoop HDFS.
- **Batch Processing**: Pyspark.
- **Data Transformation**: dbt.
- **Data Warehouse**: PostgreSQL.
- **Language Programing**: Python.

## Project Structure

```bash
Project
├── airflow             # Airflow
│   ├── dags
│   │   ├── hdfs_to_postgres.py
│   │   └── ingest_and_transform.py     # Run job Spark & dbt
│   ├── dbt_project     # dbt in Airflow
│   │   ├── macros
│   │   │   ├── .gitkeep
│   │   │   └── generate_schema_name.sql
│   │   ├── models      # transformation
│   │   │   ├── intermediate
│   │   │   │   ├── int_listen_events.sql
│   │   │   │   └── schema.yml
│   │   │   ├── marts
│   │   │   │   ├── dim_artist.sql
│   │   │   │   ├── dim_date.sql
│   │   │   │   ├── dim_location.sql
│   │   │   │   ├── dim_song.sql
│   │   │   │   ├── dim_user.sql
│   │   │   │   ├── fact.sql
│   │   │   │   └── schema.yml
│   │   │   ├── staging
│   │   │   │   ├── schema.yml
│   │   │   │   ├── stg_auth_events.sql
│   │   │   │   ├── stg_listen_events.sql
│   │   │   │   ├── stg_page_view_events.sql
│   │   │   │   └── stg_status_change_events.sql
│   │   │   └── source.yml
│   │   ├── seeds       # static data for data warehouse
│   │   │   ├── .gitkeep
│   │   │   ├── songs.csv
│   │   │   └── state_codes.csv
│   │   ├── .gitignore
│   │   ├── dbt_project.yml
│   │   └── packages.yml
│   ├── .env
│   ├── docker-compose.yaml
│   ├── Dockerfile
│   └── requirements.txt    # library for python in airflow
├── eventsim            # eventsim
│   ├── data
│   │   ├── .DS_Store
│   │   ├── Gaz_zcta_national.txt
│   │   ├── listen_counts.txt.gz
│   │   ├── songs_analysis.txt.gz
│   │   ├── Top1000Surnames.csv
│   │   ├── US.txt
│   │   ├── user agents.txt
│   │   └── yob1990.txt
│   ├── examples
│   │   ├── alt-example-config.json
│   │   └── example-config.json
│   ├── target
│   │   └── eventsim-assembly-2.0.jar
│   ├── Dockerfile
│   ├── eventsim.sh
│   └── README.md
├── hadoop              # hadoop HDFS
│   ├── conf
│   │   ├── core-site.xml
│   │   └── hdfs-site.xml
│   └── docker-compose.yaml
├── images
│   ├── Data_pipeline-graph.png
│   └── dbt_lineage.png
├── kafka               # kafka
│   └── docker-compose.yaml
├── postgres            # postgres
│   └── docker-compose.yaml
├── scripts
│   ├── start_airflow.bat
│   ├── start_eventsim.bat
│   ├── start_hadoop.bat
│   ├── start_kafka.bat
│   ├── start_postgres.bat
│   └── start_spark.bat
├── spark               # spark
│   ├── conf
│   │   └── spark-defaults.conf
│   ├── src             # Spark Streaming
│   │   ├── schema.py
│   │   ├── streaming_all_events_to_hdfs.py
│   │   └── streaming_functions.py
│   └── docker-compose.yaml
├── .gitignore
└── README.md
```

## Architecture

### Overall

![architecture](/images/architecture.png)

*Created at [excalidraw](https://excalidraw.com/)*

### Airflow graph

![aiflow-graph](/images/Data_pipeline-graph.png)

*Created by Airflow*

### dbt lineage

![dbt lineage](/images/dbt_lineage.png)

*Created by dbt*
