#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pyspark
from pyspark.sql import types
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.context import SparkContext


# In[2]:


pyspark.__file__


# In[2]:


conf = SparkConf()     .setMaster('local[*]')     .setAppName('test')     .set("spark.jars", "/home/jovyan/work/jars/spark-bigquery-latest_2.12.jar,/home/jovyan/work/jars/gcs-connector-hadoop3-latest.jar")     .set("spark.hadoop.google.cloud.auth.service.account.enable", "true")     .set("spark.hadoop.google.cloud.auth.service.account.json.keyfile", "/.google/credentials/google_credentials.json")

sc = SparkContext(conf=conf)
sc._jsc.hadoopConfiguration().set("fs.AbstractFileSystem.gs.impl",  "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
sc._jsc.hadoopConfiguration().set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
sc._jsc.hadoopConfiguration().set("fs.gs.auth.service.account.json.keyfile", "/.google/credentials/google_credentials.json")
sc._jsc.hadoopConfiguration().set("fs.gs.auth.service.account.enable", "true")


# In[3]:


# spark = SparkSession.builder \
#   .master("local[*]") \
#   .appName('1.2. BigQuery Storage & Spark SQL - Python') \
#   .config('spark.jars', '/home/jovyan/work/jars/spark-bigquery-latest.jar') \
#   .getOrCreate()
spark = SparkSession.builder     .config(conf=sc.getConf())     .getOrCreate()


# In[6]:


df_artists = spark.read.option("header",True).parquet("gs://dtc_data_lake_applied-mystery-341809/raw/artists.parquet")


# In[7]:


df_artists.printSchema()


# In[8]:


schema_artists = types.StructType([
	types.StructField('id',types.StringType(),True),
	types.StructField('followers',types.DoubleType(),True),
	types.StructField('name',types.StringType(),True)
])


# In[9]:


df_artists = spark.read .option("header",True) .schema(schema_artists) .parquet("gs://dtc_data_lake_applied-mystery-341809/raw/artists.parquet")


# In[10]:


df_artists = df_artists.repartition(8)


# In[4]:


# df_artists = spark.read \
# .option("header",True) \
# .parquet("gs://dtc_data_lake_applied-mystery-341809/transform/artists")


# In[5]:


# df_artists.show(10)


# In[11]:


# Update to your GCS bucket
gcs_bucket = 'dtc_data_lake_applied-mystery-341809'

# Update to your BigQuery dataset name you created
bq_dataset = 'bq_project_dezoomcamp'

# Enter BigQuery table name you want to create or overwite. 
# If the table does not exist it will be created when you run the write function
bq_table = 'dim_artists'

df_artists.write   .format("bigquery")   .option("table","{}.{}".format(bq_dataset, bq_table))   .option("temporaryGcsBucket", gcs_bucket)   .mode('overwrite')   .save()

