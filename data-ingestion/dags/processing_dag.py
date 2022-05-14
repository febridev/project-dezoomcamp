import os
from dataIngestion import download_from_kaggle,fhv_csv_to_parquet,upload_to_gbucket
from submitjob import submit_job,submit_job_artists,submit_job_tracks
from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
# from dotenv import load_dotenv

# load_dotenv()

# kaggle_username = os.getenv('kaggle_username')
# kaggle_key = os.getenv('kaggle_key')
PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
REGION = os.environ.get("GCP_REGION")
DATAPROC_CLUSTERNAME = os.environ.get("DATAPROC_CLUSTERNAME")
AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
csv_source = AIRFLOW_HOME+'/kaggle'



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
        python_callable=download_from_kaggle,
        op_kwargs={
            "downloadpath": csv_source
        }
    )

    local_to_gcs_task = PythonOperator(
        task_id="local_to_gcs_task",
        python_callable=upload_to_gbucket,
        op_kwargs={
            "bucket": BUCKET,
            "srcfile": f"{csv_source}",
        }
    )

    # submitjob=PythonOperator(
    #     task_id="submitjob",
    #     retries=1,
    #     python_callable=submit_job,
    #     op_kwargs=dict(
    #         project_id = PROJECT_ID,
    #         region = REGION,
    #         cluster_name = DATAPROC_CLUSTERNAME
    #     )
    # )
    job_dim_artists=PythonOperator(
        task_id="job_dim_artists",
        retries=1,
        python_callable=submit_job_artists,
        op_kwargs=dict(
            project_id = PROJECT_ID,
            region = REGION,
            cluster_name = DATAPROC_CLUSTERNAME,
            appname = 'job_dim_artists',
            gcs_bucket_name = BUCKET,
            filename=f"gs://{BUCKET}/code/bq_dim_artists.py"
        )
    )

    # job_fact_tracks=PythonOperator(
    #     task_id="job_fact_tracks",
    #     retries=1,
    #     python_callable=submit_job_tracks,
    #     op_kwargs=dict(
    #         project_id = PROJECT_ID,
    #         region='asia-southeast2',
    #         cluster_name='project-dezoomcamp',
    #         appname='job_fact_tracks',
    #         gcs_bucket_name=BUCKET,
    #         filename=f"gs://{BUCKET}/code/bq_fact_tracks.py"
    #     )
    # )
    downloadDataset >> local_to_gcs_task >> job_dim_artists 
    # >> job_dim_artists >> job_fact_tracks