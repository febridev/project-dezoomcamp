#!/usr/bin/env python
# coding: utf-8
import pyspark
from pyspark.sql import types
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.context import SparkContext

# conf = SparkConf() \
#        .setMaster('spark://spark:7077') \
#        .setAppName('demo') \
#        .set("spark.jars", "/usr/local/spark/resources/jars/gcs-connector-hadoop3-latest.jar") \
#        .set("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
#        .set("spark.hadoop.google.cloud.auth.service.account.json.keyfile", "/.google/credentials/google_credentials.json")

# sc = SparkContext(conf=conf)
# sc._jsc.hadoopConfiguration().set("fs.AbstractFileSystem.gs.impl","com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
# sc._jsc.hadoopConfiguration().set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
# sc._jsc.hadoopConfiguration().set("fs.gs.auth.service.account.json.keyfile", "/.google/credentials/google_credentials.json")
# sc._jsc.hadoopConfiguration().set("fs.gs.auth.service.account.enable", "true")

# spark = SparkSession.builder \
#     .appName('demo')
#     .config(conf=sc.getConf()) \
#     .getOrCreate()

spark = SparkSession.builder \
    .appName('demo') \
    .getOrCreate()


df_artists = spark.read.option("header",True).parquet("gs://dtc_data_lake_applied-mystery-341809/raw/artists.parquet")
df_artists.show(1)

# table = "applied-mystery-341809.dbt_week5.dim_zones"
# df_wiki_pageviews = spark.read \
#                     .format("bigquery") \
#                     .option("table", table) \
#                     .load()

# df_wiki_pageviews.show(10)