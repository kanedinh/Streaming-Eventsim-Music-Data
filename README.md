# Streaming 'Eventsim' Music Data

A data pipeline with Kafka, Spark, Hadoop, dbt, Docker, Airflow and much more!

## Objective

The project will stream events generated from a fake music streaming service (like Spotify) and create a data pipeline that consumes the real-time data. In the end of the day, the batch job will consume the data of that day, apply transformations and create a data warehouse.

## About Data

Eventsim is the program that generates event data from a fake music website. Read more [in here](https://github.com/Interana/eventsim). 

The docker image is borrowed from [viirya's fork](https://github.com/viirya/eventsim).

Eventsim uses song data from [Million Songs Dataset](http://millionsongdataset.com) to generate events. I have used a [subset](http://millionsongdataset.com/pages/getting-dataset/#subset) of 10000 songs.

## What's Tools & Technologies I used in this Project

- **Data source**: Eventsim
- **Containerization**: Docker
- **Stream Processing**: Kafka, Spark Streaming
- **Data Storage**: Hadoop HDFS
- **Batch Processing**: Pyspark
- **Data Transformation**: dbt
- **Data Warehouse**: PostgreSQL
- **Language Programing**: Python

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
│   ├── examples
│   ├── target
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

## Quick Guide For Windows

Requirements: 8 Cores CPU & 16GB RAM

**First**, install [Docker Desktop](https://www.docker.com/).

**Second**, 'streaming & load' stage. You need to start some containers and create a docker network ```shared_network``` for this stage. (kafka, spark, hadoop). You may run ```start_kafka.bat```, ```start_spark.bat```, ... in scripts folder OR run the scripts below in terminal.

```shell
docker create network shared_network
cd kafka
docker-compose up -d
cd ..\spark
docker-compose up -d
cd ..\hadoop
docker-compose up -d
```

About Kafka, you can access [localhost:9021](localhost:9021) to see Control Center of it.

About Spark, go to [localhost:9080](localhost:9080) to see its.

About hadoop, go to [localhost:9870](localhost:9870) to access namenode.

If you access all the services above, you may simulate events data. Run ```start_eventsim.bat``` to begin. You can config some variables like:

- "DAY_FROM_NOW": from x day ago

- "NUSERS" : initial number of users

- "GROWTH_RATE": annual user growth rate

**Third**, 'transform' stage. You need to start airflow & postgres containers (```start_airflow.bat``` & ```start_postgres.bat```)

Add **spark** & **postgres** connections to airflow.

![Connections](/images/airflow_connection.png)

After that, you can run DAG with name "Data_pipeline".
