
1. ![dataproc_1](https://github.com/febridev/project-dezoomcamp/blob/main/screenshoot/dataproc_1.jpg)
2. ![dataproc_2](https://github.com/febridev/project-dezoomcamp/blob/main/screenshoot/dataproc_2.jpg)
3. ![dataproc_3](https://github.com/febridev/project-dezoomcamp/blob/main/screenshoot/dataproc_3.jpg)
4. ![dataproc_4](https://github.com/febridev/project-dezoomcamp/blob/main/screenshoot/dataproc_4.jpg)
5. ![dataproc_5](https://github.com/febridev/project-dezoomcamp/blob/main/screenshoot/dataproc_5.jpg)
6. ![dataproc_6](https://github.com/febridev/project-dezoomcamp/blob/main/screenshoot/dataproc_6.jpg)
7. ![dataproc_7](https://github.com/febridev/project-dezoomcamp/blob/main/screenshoot/dataproc_7.jpg)
8. ![dataproc_8](https://github.com/febridev/project-dezoomcamp/blob/main/screenshoot/dataproc_8.jpg)
9. ![dataproc_9](https://github.com/febridev/project-dezoomcamp/blob/main/screenshoot/dataproc_9.jpg)

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
