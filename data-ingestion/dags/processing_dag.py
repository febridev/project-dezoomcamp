import os
from dataIngestion import download_from_kaggle,fhv_csv_to_parquet
from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
# from dotenv import load_dotenv

# load_dotenv()

# kaggle_username = os.getenv('kaggle_username')
# kaggle_key = os.getenv('kaggle_key')

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
csv_source = AIRFLOW_HOME+'/kaggle'
list_csv_file = os.listdir(csv_source)


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
    for lfile in list_csv_file:
        csv_to_parquet=PythonOperator(
            task_id="convert_parquet",
            retries=1,
            python_callable=fhv_csv_to_parquet,
            op_kwargs=dict(
                srcfile = f'{csv_source}/{lfile}'
            )
        )

    downloadDataset >> csv_to_parquet