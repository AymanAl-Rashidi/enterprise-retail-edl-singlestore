import os
import subprocess

from datetime import datetime

from airflow.sdk import dag, task


PROJECT_PATH = "/opt/airflow/project"


def run_script(script_path):
    env = os.environ.copy()

    subprocess.run(
        ["python", script_path],
        check=True,
        env=env
    )


@dag(
    dag_id="retail_edl_batch_pipeline",
    schedule=None,
    start_date=datetime(2026, 10, 1),
    catchup=False,
    tags=["retail", "edl", "batch"]
)
def retail_edl_batch_pipeline():

    @task
    def ingest_bronze():
        run_script(
            f"{PROJECT_PATH}/scripts/ingest_bronze.py"
        )

    @task
    def bronze_to_silver():
        run_script(
            f"{PROJECT_PATH}/spark/bronze_to_silver.py"
        )

    @task
    def silver_to_gold():
        run_script(
            f"{PROJECT_PATH}/spark/silver_to_gold.py"
        )

    @task
    def download_gold():
        run_script(
            f"{PROJECT_PATH}/scripts/download_gold.py"
        )

    @task
    def load_gold_to_singlestore():
        run_script(
            f"{PROJECT_PATH}/scripts/load_gold_to_singlestore.py"
        )


    bronze = ingest_bronze()
    silver = bronze_to_silver()
    gold = silver_to_gold()
    download = download_gold()
    load = load_gold_to_singlestore()

    bronze >> silver >> gold >> download >> load


retail_edl_batch_pipeline()