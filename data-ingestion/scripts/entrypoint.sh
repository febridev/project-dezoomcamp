#!/usr/bin/env bash
export GOOGLE_APPLICATION_CREDENTIALS=${GOOGLE_APPLICATION_CREDENTIALS}
export AIRFLOW_CONN_GOOGLE_CLOUD_DEFAULT=${AIRFLOW_CONN_GOOGLE_CLOUD_DEFAULT}
export KAGGLE_USERNAME=febridev
export KAGGLE_KEY=591bc2adbb0869b1c8cff89f18fe5ec0

pip install kaggle

airflow db upgrade

airflow users create -r Admin -u admin -p admin -e admin@example.com -f admin -l airflow
# "$_AIRFLOW_WWW_USER_USERNAME" -p "$_AIRFLOW_WWW_USER_PASSWORD"

airflow webserver