from datetime import datetime, timedelta
import pendulum
from airflow.sdk import dag, task
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos.profiles import PostgresUserPasswordProfileMapping

# timezone
local_tz = pendulum.timezone("Asia/Ho_Chi_Minh")

# profile for dbt
profile_config = ProfileConfig(
    profile_name="Me",
    target_name="dev",
    profile_mapping=PostgresUserPasswordProfileMapping(
        conn_id="my_postgres_conn",
        profile_args={"schema": "public"}
    )
)

@dag(
    dag_id="Data_pipeline_v2",
    schedule='@daily',
    start_date= pendulum.datetime(2026, 1, 1, tz=local_tz), # change it according ts of event
    catchup=False,
    max_active_runs=1,
)
def my_dag():
    hdfs_to_postgres = SparkSubmitOperator(
        task_id="HDFS_To_Postgres",
        application='/opt/airflow/dags/hdfs_to_postgres.py', 
        conn_id='spark_default',
        name='HDFS_To_Postgres',
        verbose=True,
        application_args=['--date', '{{ ds }}'],
        conf = {
            # which container run the session
            'spark.driver.host': 'airflow-scheduler',
            'spark.driver.bindAddress': '0.0.0.0', 

            # enable eventLog that run the session. Use localhost:18080 to see it after finishing the session
            'spark.eventLog.enabled': 'true',
            'spark.eventLog.dir': 'file:///tmp/spark-events',
            'spark.history.fs.logDirectory': 'file:///tmp/spark-events',

            # config executor's power
            'spark.executor.memory': '2g',
            'spark.executor.cores': '2',

            # packages for pyspark
            'spark.jars.packages': 'org.postgresql:postgresql:42.6.0',
        }
    )

    dbt_transform = DbtTaskGroup(
        group_id="run_dbt_models",
        # project for dbt
        project_config=ProjectConfig(
            dbt_project_path="/opt/airflow/dbt_project",
        ),
        # profile for dbt
        profile_config=profile_config,
        execution_config=ExecutionConfig(
            dbt_executable_path="dbt",
        ),
        # runing 'dbt deps' before running the task
        operator_args={
            "install_deps":True,
        }
    )
    
    @task.python
    def notify_success():
        print("Pipeline completely!!!")

    notify_task = notify_success()

    hdfs_to_postgres >> dbt_transform >> notify_task

my_pipeline = my_dag()