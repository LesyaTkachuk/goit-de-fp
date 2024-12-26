from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

# define default arguments for DAG
default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 12, 24, 0, 0),
}

FILES_PATH = "/opt/airflow/task_2"

# define Dag
with DAG(
    "data_landing_to_gold_processing_oleksandratk",
    default_args=default_args,
    schedule=None,
    catchup=False,
    tags=["oleksandratk"],
) as dag:
    landing_to_bronze_task = SparkSubmitOperator(
        task_id="landing_to_bronze",
        application=f"{FILES_PATH}/landing_to_bronze.py",
        conf={"spark.yarn.resourceManager.address": "your_resource_manager_host:8032"},
        name="landing_to_bronze",
        conn_id="spark_default",
    )

    bronze_to_silver_task = SparkSubmitOperator(
        task_id="bronze_to_silver",
        application=f"{FILES_PATH}/bronze_to_silver.py",
        name="bronze_to_silver",
        conf={"spark.yarn.resourceManager.address": "your_resource_manager_host:8032"},
        conn_id="spark_default",
    )

    silver_to_gold_task = SparkSubmitOperator(
        task_id="silver_to_gold",
        application=f"{FILES_PATH}/silver_to_gold.py",
        name="silver_to_gold",
        conf={"spark.yarn.resourceManager.address": "your_resource_manager_host:8032"},
        conn_id="spark_default",
    )

    # set dependencies
    landing_to_bronze_task >> bronze_to_silver_task >> silver_to_gold_task
