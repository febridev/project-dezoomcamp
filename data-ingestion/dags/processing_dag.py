import os
from dataIngestion import download_from_kaggle
from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
# from dotenv import load_dotenv

# load_dotenv()

# kaggle_username = os.getenv('kaggle_username')
# kaggle_key = os.getenv('kaggle_key')


local_workflow = DAG(
    "dezoomcampProject",
    schedule_interval="@once",
    catchup = False,
    start_date = datetime(2022,3,26),
    max_active_runs = 3
)

with local_workflow:
    downloadDataset=PythonOperator(
        task_id="dataset_download",
        retries=1,
        python_callable=download_from_kaggle
    )

    downloadDataset