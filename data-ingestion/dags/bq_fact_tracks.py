#!/usr/bin/env python
# coding: utf-8
import argparse
import pyspark
from pyspark.sql import types
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.context import SparkContext


parser = argparse.ArgumentParser()

parser.add_argument('--appname', required=True)
parser.add_argument('--gcs_bucket_name', required=True)

args = parser.parse_args()

appname = args.appname
gcs_bucket_name = args.gcs_bucket_name

fullpath = f"gs://{gcs_bucket_name}/raw/tracks.csv"

spark = SparkSession.builder.appName(appname).getOrCreate()

df_tracks = spark.read.option("header",True).csv(fullpath)
df_tracks.printSchema()
df_tracks.schema

schema = types.StructType([
	types.StructField('id',types.StringType(),True),
	types.StructField('name',types.StringType(),True),
	types.StructField('popularity',types.IntegerType(),True),
	types.StructField('duration_ms',types.IntegerType(),True),
	types.StructField('explicit',types.IntegerType(),True),
	types.StructField('artists',types.StringType(),True),
	types.StructField('id_artists',types.StringType(),True),
	types.StructField('release_date',types.TimestampType(),True),
	types.StructField('danceability',types.FloatType(),True),
	types.StructField('energy',types.FloatType(),True),
	types.StructField('key',types.IntegerType(),True),
	types.StructField('loudness',types.FloatType(),True),
	types.StructField('mode',types.IntegerType(),True),
	types.StructField('speechiness',types.FloatType(),True),
	types.StructField('acousticness',types.FloatType(),True),
	types.StructField('instrumentalness',types.FloatType(),True),
	types.StructField('liveness',types.FloatType(),True),
	types.StructField('valence',types.FloatType(),True),
	types.StructField('tempo',types.FloatType(),True),
	types.StructField('time_signature',types.IntegerType(),True)
])

df_tracks = spark.read.option("header",True).schema(schema).csv(fullpath)
df_tracks = df_tracks.repartition(8)

# Update to your GCS bucket
gcs_bucket = gcs_bucket_name

# Update to your BigQuery dataset name you created
bq_dataset = 'bq_project_dezoomcamp'

# Enter BigQuery table name you want to create or overwite. 
# If the table does not exist it will be created when you run the write function
bq_table = 'fact_tracks'

df_tracks.write \
.format("bigquery") \
.option("table","{}.{}".format(bq_dataset, bq_table)) \
.option("temporaryGcsBucket", gcs_bucket) \
.mode('overwrite') \
.save()

