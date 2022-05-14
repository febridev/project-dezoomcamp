import re


from google.cloud import dataproc_v1 as dataproc
from google.cloud import storage


def submit_job(project_id, region, cluster_name):
    # Create the job client.
    job_client = dataproc.JobControllerClient(
        client_options={"api_endpoint": "{}-dataproc.googleapis.com:443".format(region)}
    )

    # Create the job config. 'main_jar_file_uri' can also be a
    # Google Cloud Storage URL.
    job = {
        "placement": {"cluster_name": cluster_name},
        "pyspark_job": {
            "main_python_file_uri":"gs://dtc_data_lake_applied-mystery-341809/code/test-spark.py"
        },
    }

    operation = job_client.submit_job_as_operation(
        request={"project_id": project_id, "region": region, "job": job}
    )
    response = operation.result()

    # Dataproc job output gets saved to the Google Cloud Storage bucket
    # allocated to the job. Use a regex to obtain the bucket and blob info.
    matches = re.match("gs://(.*?)/(.*)", response.driver_output_resource_uri)

    output = (
        storage.Client()
        .get_bucket(matches.group(1))
        .blob(f"{matches.group(2)}.000000000")
        .download_as_string()
    )

    print(f"Job finished successfully: {output}")


def submit_job_artists(project_id, region, cluster_name, appname, gcs_bucket_name,filename):
    # Create the job client.
    job_client = dataproc.JobControllerClient(
        client_options={"api_endpoint": "{}-dataproc.googleapis.com:443".format(region)}
    )

    # Create the job config. 'main_jar_file_uri' can also be a
    # Google Cloud Storage URL.
    job = {
        "placement": {"cluster_name": cluster_name},
        "pyspark_job": {
            "main_python_file_uri": filename,
            "jar_file_uris": [
                                # "gs://".dtc_data_lake_applied-mystery-341809."/code/jars/gcs-connector-hadoop3-latest.jar",
                                "gs://"+gcs_bucket_name+"/code/jars/gcs-connector-hadoop3-latest.jar",
                                "gs://"+gcs_bucket_name+"/code/jars/spark-bigquery-latest_2.12.jar"
            ],
            "args": [
                        "--appname="+appname,
                        "--gcs_bucket_name="+gcs_bucket_name
            ]
        },
    }

    operation = job_client.submit_job_as_operation(
        request={"project_id": project_id, "region": region, "job": job}
    )
    response = operation.result()

    # Dataproc job output gets saved to the Google Cloud Storage bucket
    # allocated to the job. Use a regex to obtain the bucket and blob info.
    matches = re.match("gs://(.*?)/(.*)", response.driver_output_resource_uri)

    output = (
        storage.Client()
        .get_bucket(matches.group(1))
        .blob(f"{matches.group(2)}.000000000")
        .download_as_string()
    )

    print(f"Job finished successfully: {output}")


def submit_job_tracks(project_id, region, cluster_name, appname, gcs_bucket_name,filename):
    # Create the job client.
    job_client = dataproc.JobControllerClient(
        client_options={"api_endpoint": "{}-dataproc.googleapis.com:443".format(region)}
    )

    # Create the job config. 'main_jar_file_uri' can also be a
    # Google Cloud Storage URL.
    job = {
        "placement": {"cluster_name": cluster_name},
        "pyspark_job": {
            "main_python_file_uri":filename,
            "jar_file_uris": [
                                "gs://dtc_data_lake_applied-mystery-341809/code/jars/gcs-connector-hadoop3-latest.jar",
                                "gs://dtc_data_lake_applied-mystery-341809/code/jars/spark-bigquery-latest_2.12.jar"
            ],
            "args": [
                        "--appname=job_dim_artists",
                        "--gcs_bucket_name=dtc_data_lake_applied-mystery-341809"
            ]
        },
    }

    operation = job_client.submit_job_as_operation(
        request={"project_id": project_id, "region": region, "job": job}
    )
    response = operation.result()

    # Dataproc job output gets saved to the Google Cloud Storage bucket
    # allocated to the job. Use a regex to obtain the bucket and blob info.
    matches = re.match("gs://(.*?)/(.*)", response.driver_output_resource_uri)

    output = (
        storage.Client()
        .get_bucket(matches.group(1))
        .blob(f"{matches.group(2)}.000000000")
        .download_as_string()
    )

    print(f"Job finished successfully: {output}")
