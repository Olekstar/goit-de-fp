"""
project_solution.py
DAG, що послідовно запускає landing_to_bronze.py,
bronze_to_silver.py, silver_to_gold.py
"""

from datetime import datetime
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 1, 1),
    'retries': 0
}

connection_id = 'spark-default'  # назва Spark Connection у Airflow

with DAG(
    dag_id='final_project_oleksandr_s',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["oleksandr_s"],
) as dag:

    # 1. landing_to_bronze
    landing_to_bronze = SparkSubmitOperator(
        task_id='landing_to_bronze',
        application='dags/oleksandr_s_spark_jobs/landing_to_bronze.py',
        conn_id=connection_id,
        verbose=True,
    )

    # 2. bronze_to_silver
    bronze_to_silver = SparkSubmitOperator(
        task_id='bronze_to_silver',
        application='dags/oleksandr_s_spark_jobs/bronze_to_silver.py',
        conn_id=connection_id,
        verbose=True,
    )

    # 3. silver_to_gold
    silver_to_gold = SparkSubmitOperator(
        task_id='silver_to_gold',
        application='dags/oleksandr_s_spark_jobs/silver_to_gold.py',
        conn_id=connection_id,
        verbose=True,
    )

    # Послідовність виконання
    landing_to_bronze >> bronze_to_silver >> silver_to_gold
