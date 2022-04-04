# First-time build can take upto 10 mins.

FROM apache/airflow:2.2.3

ENV AIRFLOW_HOME=/opt/airflow
ENV KAGGLE_USERNAME=febridev
ENV KAGGLE_KEY=591bc2adbb0869b1c8cff89f18fe5ec0

USER root
RUN apt-get update -qq && apt-get install vim -qqq && apt-get install wget -qq
# git gcc g++ -qqq

COPY requirements.txt .
RUN python -m pip install --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt



# Ref: https://airflow.apache.org/docs/docker-stack/recipes.html

SHELL ["/bin/bash", "-o", "pipefail", "-e", "-u", "-x", "-c"]

ARG CLOUD_SDK_VERSION=322.0.0
ENV GCLOUD_HOME=/home/google-cloud-sdk

ENV PATH="${GCLOUD_HOME}/bin/:${PATH}"

RUN DOWNLOAD_URL="https://dl.google.com/dl/cloudsdk/channels/rapid/downloads/google-cloud-sdk-${CLOUD_SDK_VERSION}-linux-x86_64.tar.gz" \
    && TMP_DIR="$(mktemp -d)" \
    && curl -fL "${DOWNLOAD_URL}" --output "${TMP_DIR}/google-cloud-sdk.tar.gz" \
    && mkdir -p "${GCLOUD_HOME}" \
    && tar xzf "${TMP_DIR}/google-cloud-sdk.tar.gz" -C "${GCLOUD_HOME}" --strip-components=1 \
    && "${GCLOUD_HOME}/install.sh" \
       --bash-completion=false \
       --path-update=false \
       --usage-reporting=false \
       --quiet \
    && rm -rf "${TMP_DIR}" \
    && gcloud --version

###############################
## Begin JAVA installation
###############################
# Java is required in order to spark-submit work
# Install OpenJDK-8
RUN apt-get update && \
    apt-get install -y software-properties-common && \
    apt-get install -y gnupg2 && \
    apt-key adv --keyserver keyserver.ubuntu.com --recv-keys EB9B1D8886F44E2A && \
    add-apt-repository "deb http://security.debian.org/debian-security stretch/updates main" && \ 
    apt-get update && \
    apt-get install -y openjdk-8-jdk && \
    pip freeze && \
    java -version $$ \
    javac -version

# Setup JAVA_HOME 
ENV JAVA_HOME /usr/lib/jvm/java-8-openjdk-amd64
RUN export JAVA_HOME
###############################
## Finish JAVA installation
###############################


###############################
## SPARK files and variables
###############################
ENV SPARK_HOME /usr/local/spark



# Spark submit binaries and jars (Spark binaries must be the same version of spark cluster)
RUN cd "/tmp" && \
        wget --no-verbose "https://archive.apache.org/dist/spark/spark-3.1.2/spark-3.1.2-bin-hadoop3.2.tgz" && \
        tar -xvzf "spark-3.1.2-bin-hadoop3.2.tgz" && \
        mkdir -p "${SPARK_HOME}/bin" && \
        mkdir -p "${SPARK_HOME}/assembly/target/scala-2.12/jars" && \
        cp -a "spark-3.1.2-bin-hadoop3.2/bin/." "${SPARK_HOME}/bin/" && \
        cp -a "spark-3.1.2-bin-hadoop3.2/jars/." "${SPARK_HOME}/assembly/target/scala-2.12/jars/" && \
        rm "spark-3.1.2-bin-hadoop3.2.tgz"

COPY gcs-connector-hadoop3-latest.jar /usr/local/spark/jars/gcs-connector-hadoop3-latest.jar
COPY spark-bigquery-latest_2.12.jar /usr/local/spark/jars/spark-bigquery-latest_2.12.jar

# Create SPARK_HOME env var
RUN export SPARK_HOME
ENV PATH $PATH:/usr/local/spark/bin
###############################
## Finish SPARK files and variables
###############################



WORKDIR $AIRFLOW_HOME
COPY scripts scripts
RUN chmod +x scripts
USER $AIRFLOW_UID

