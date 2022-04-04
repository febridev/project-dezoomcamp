#!/usr/bin/env python
# coding: utf-8

# In[2]:


import pyspark
from pyspark.sql import types
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.context import SparkContext


# In[3]:


conf = SparkConf()     .setMaster('local[*]')     .setAppName('test')     .set("spark.jars", "../spark/resources/jars/gcs-connector-hadoop3-latest.jar,../spark/resources/jars/spark-bigquery-latest_2.12.jar")     .set("spark.hadoop.google.cloud.auth.service.account.enable", "true")     .set("spark.hadoop.google.cloud.auth.service.account.json.keyfile", "/.google/credentials/google_credentials.json")

sc = SparkContext(conf=conf)
sc._jsc.hadoopConfiguration().set("fs.AbstractFileSystem.gs.impl",  "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
sc._jsc.hadoopConfiguration().set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
sc._jsc.hadoopConfiguration().set("fs.gs.auth.service.account.json.keyfile", "/.google/credentials/google_credentials.json")
sc._jsc.hadoopConfiguration().set("fs.gs.auth.service.account.enable", "true")


# In[4]:


spark = SparkSession.builder     .config(conf=sc.getConf())     .getOrCreate()


# In[5]:


df_tracks = spark.read.option("header",True).parquet("gs://dtc_data_lake_applied-mystery-341809/raw/tracks.parquet")


# In[8]:


df_tracks.printSchema()


# In[9]:


df_tracks.schema


# In[10]:


df_tracks.show(10)


# In[6]:


schema = types.StructType([
	types.StructField('id',types.StringType(),True),
	types.StructField('name',types.StringType(),True),
	types.StructField('popularity',types.LongType(),True),
	types.StructField('duration_ms',types.LongType(),True),
	types.StructField('explicit',types.LongType(),True),
	types.StructField('artists',types.StringType(),True),
	types.StructField('id_artists',types.StringType(),True),
	types.StructField('release_date',types.TimestampType(),True),
	types.StructField('danceability',types.DoubleType(),True),
	types.StructField('energy',types.DoubleType(),True),
	types.StructField('key',types.LongType(),True),
	types.StructField('loudness',types.DoubleType(),True),
	types.StructField('mode',types.LongType(),True),
	types.StructField('speechiness',types.DoubleType(),True),
	types.StructField('acousticness',types.DoubleType(),True),
	types.StructField('instrumentalness',types.DoubleType(),True),
	types.StructField('liveness',types.DoubleType(),True),
	types.StructField('valence',types.DoubleType(),True),
	types.StructField('tempo',types.DoubleType(),True),
	types.StructField('time_signature',types.LongType(),True)
])


# In[7]:


df_tracks = spark.read .option("header",True) .schema(schema) .parquet("gs://dtc_data_lake_applied-mystery-341809/raw/tracks.parquet")


# In[8]:


df_tracks.printSchema()


# In[9]:


df_tracks = df_tracks.repartition(8)


# In[10]:


# Update to your GCS bucket
gcs_bucket = 'dtc_data_lake_applied-mystery-341809'

# Update to your BigQuery dataset name you created
bq_dataset = 'bq_project_dezoomcamp'

# Enter BigQuery table name you want to create or overwite. 
# If the table does not exist it will be created when you run the write function
bq_table = 'fact_tracks'

df_tracks.write   .format("bigquery")   .option("table","{}.{}".format(bq_dataset, bq_table))   .option("temporaryGcsBucket", gcs_bucket)   .mode('overwrite')   .save()

