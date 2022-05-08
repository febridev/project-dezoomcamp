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

## Technology choices
1. Cloud: GCP
2. Datalake: GCP Bucket
3. Infrastructure as code (IaC): Terraform 
4. Workflow orchestration: Airflow 
5. Data Warehouse: BigQuery 
6. Transformation: Google Cloud Dataproc

## Installation Google Cloud Infrastructure Using Terraform

```shell
# Refresh service-account's auth-token for this session
gcloud auth application-default login --no-launch-browser

# Initialize state file (.tfstate)
cd terraform/
terraform init

# Check changes to new infra plan
terraform plan -var="project=<your project id>" \
-var="region=<your region>" \
-var="BQ_DATASET=<datsetname on bigquery>" \
-var="DATAPROC_CLUSTERNAME=<dataproc clustername>" \
-var="SERVICE_ACCOUNT=<service-account-from-iam>"
```

```shell
# Create new infra
terraform apply -var="project=<your project id>" \
-var="region=<your region>" \
-var="BQ_DATASET=<datsetname on bigquery>" \
-var="DATAPROC_CLUSTERNAME=<dataproc clustername>" \
-var="SERVICE_ACCOUNT=<service-account-from-iam>"

```

```shell
# Delete infra after your work, to avoid costs on any running services
terraform destroy -var="project=<your project id>" 
```
## Installation Google Dataproc
Because I already tried build data proc using terraform sucessfullt but when I running the pyspark job is still error. and I build google dataproc using wizard from gcp console and running the pyspark job is successfully, I recommended build the google dataproc using wizard from gcp console.

#### This is Command Line If create data proc from gcloud command 
```shell
gcloud dataproc clusters create project-dezoomcamp --region asia-southeast2 --zone asia-southeast2-c --single-node --master-machine-type n1-standard-4 --master-boot-disk-size 500 --image-version 2.0-debian10 --optional-components JUPYTER,DOCKER --max-idle 604800s --project applied-mystery-341809
```

#### This is if using REST API
```json
POST /v1/projects/applied-mystery-341809/regions/asia-southeast2/clusters/
{
  "projectId": "applied-mystery-341809",
  "clusterName": "project-dezoomcamp",
  "config": {
    "configBucket": "",
    "gceClusterConfig": {
      "networkUri": "default",
      "subnetworkUri": "",
      "internalIpOnly": false,
      "zoneUri": "asia-southeast2-c",
      "metadata": {},
      "tags": [],
      "shieldedInstanceConfig": {
        "enableSecureBoot": false,
        "enableVtpm": false,
        "enableIntegrityMonitoring": false
      }
    },
    "masterConfig": {
      "numInstances": 1,
      "machineTypeUri": "n1-standard-4",
      "diskConfig": {
        "bootDiskType": "pd-standard",
        "bootDiskSizeGb": 500,
        "numLocalSsds": 0
      },
      "minCpuPlatform": "",
      "imageUri": ""
    },
    "softwareConfig": {
      "imageVersion": "2.0-debian10",
      "properties": {
        "dataproc:dataproc.allow.zero.workers": "true"
      },
      "optionalComponents": [
        "JUPYTER",
        "DOCKER"
      ]
    },
    "lifecycleConfig": {
      "idleDeleteTtl": "604800s"
    },
    "initializationActions": [],
    "encryptionConfig": {
      "gcePdKmsKeyName": ""
    },
    "autoscalingConfig": {
      "policyUri": ""
    },
    "endpointConfig": {
      "enableHttpPortAccess": false
    },
    "securityConfig": {
      "kerberosConfig": {}
    }
  },
  "labels": {},
  "status": {},
  "statusHistory": [
    {}
  ],
  "metrics": {}
}
```

## Copy File And Jar to Google Cloud Storage For Run DataProc

```shell
gsutil cp data-ingestion/dags/bq_dim_artists.py gs://dtc_data_lake_applied-mystery-341809/code/bq_dim_artists.py
gsutil cp data-ingestion/dags/bq_dim_artists.py gs://dtc_data_lake_applied-mystery-341809/code/bq_fact_tracks.py
gsutil cp data-ingestion/spark/resources/jars/*.jar gs://dtc_data_lake_applied-mystery-341809/code/jars

```
## Installation Airflow

```shell
docker-compose up 
```

Airflow Webserver
http://localhost:8090

```shell
user: admin
password : admin
```




## Dashboard
1. Total number of tracks
2. Total number of artists
3. Most popular song - by popularity
4. Most popular artist - by followers
5. Most Tracks - Sort by Artist