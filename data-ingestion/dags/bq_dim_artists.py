#!/usr/bin/env python
# coding: utf-8
import argparse
import pyspark
import pyspark.sql.functions as f
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

fullpath = f"gs://{gcs_bucket_name}/raw/artists.csv"
cleanpath = f"gs://{gcs_bucket_name}/raw/artists_clean"

spark = SparkSession.builder.appName(appname).getOrCreate()

df_artists = spark.read.option("header",True).csv(fullpath)

# cleansing data 
df_artists = df_artists.select("id","followers","name","popularity")
df_artists.write.mode('overwrite').option("header", "true").csv(cleanpath)

# end cleansing

# df_artists.printSchema()

# schema_artists = types.StructType([
# 	types.StructField('id',types.StringType(),True),
# 	types.StructField('followers',types.DoubleType(),True),
# 	types.StructField('name',types.StringType(),True)
# ])

schema_artists = types.StructType([
	types.StructField('id',types.StringType(),True),
	types.StructField('followers',types.DoubleType(),True),
	types.StructField('name',types.StringType(),True),
    types.StructField('popularity',types.IntegerType(),True)
])

df_artists = spark.read.option("header",True).schema(schema_artists).csv(cleanpath)
df_artists = df_artists.repartition(8)

gcs_bucket = gcs_bucket_name
# Update to your BigQuery dataset name you created
bq_dataset = 'bq_project_dezoomcamp'

# Enter BigQuery table name you want to create or overwite. 
# If the table does not exist it will be created when you run the write function
bq_table = 'dim_artists'

df_artists.write \
	.format("bigquery") \
	.option("table","{}.{}".format(bq_dataset, bq_table)) \
	.option("temporaryGcsBucket", gcs_bucket) \
	.mode('overwrite') \
	.save()

