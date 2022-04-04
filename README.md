# project-dezoomcamp
Project For dezoomcamp with datatalks club



## Background Issue 
(This is a hypothetical and synthetic requirement formulated for the zoomcamp project).

Make some data pipeline from dataset Spotify tracks and Get some Popularity Tracks, Artist or etc and put all the information to dashboard for stakeholder 

## Project high level design
This project produces a pipeline which:

1. Build cloud infrastructure using Terraform 
2. pull the raw data into GCP cloud
3. Transforms the raw data 
4. Joins the artists and tracks table to provide popularity write them back into BigQuery
5. Produce dashboard tiles in Google Data studio.
6. This allows the analytics to view the combined tracks and artists popularity information for quick review.

## Dataset
[Spotify Dataset](https://www.kaggle.com/yamaerenay/spotify-dataset-19212020-600k-tracks?select=tracks.csv)

## Installation Terraform

```shell
# Refresh service-account's auth-token for this session
gcloud auth application-default login

# Initialize state file (.tfstate)
terraform init

# Check changes to new infra plan
terraform plan -var="project=<your-gcp-project-id>"
```

```shell
# Create new infra
terraform apply -var="project=<your-gcp-project-id>"
```

```shell
# Delete infra after your work, to avoid costs on any running services
terraform destroy
```

## Installation Airflow

```shell
docker-compose up 
```

[Airflow WebServer](http://localhost:8080)

```shell
user: admin
password : admin
```


## Technology choices
1. Cloud: GCP
2. Datalake: GCP Bucket
3. Infrastructure as code (IaC): Terraform 
4. Workflow orchestration: Airflow 
5. Data Warehouse: BigQuery 
6. Transformation: Google Cloud Dataproc

## Dashboard
1. Total number of tracks
2. Total number of artists
3. Most popular song - by popularity
4. Most popular artist - by followers
5. Most Tracks - Sort by Artist