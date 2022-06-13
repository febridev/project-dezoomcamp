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

fullpath = f"gs://{gcs_bucket_name}/raw/tracks.csv"
cleanpath = f"gs://{gcs_bucket_name}/raw/most_tracks_clean"

spark = SparkSession.builder.appName(appname).getOrCreate()

df_tracks = spark.read.option("header",True).csv(fullpath)

#cleansing data 
df_tracks = df_tracks.select(df_tracks.id, df_tracks.name,f.translate(f.col("id_artists"),"[\\[]\\'']", "").alias("id_artists"))

# set column id_artists as array
df_tracks_cl = df_tracks.select(df_tracks.id, df_tracks.name, f.split(df_tracks.id_artists,",").alias("artists_array"))
df_tracks_cl.printSchema()

#explode array to row 
df2 = df_tracks_cl.select(df_tracks_cl.id, df_tracks_cl.name, f.explode(df_tracks_cl.artists_array).alias("artists_array"))

#write new csv file after explode array
df2.write.mode('overwrite').option("header", "true").csv(cleanpath)
df2.schema

schema = types.StructType([
	types.StructField('id',types.StringType(),True),
	types.StructField('name',types.StringType(),True),
    types.StructField('artists_array',types.StringType(),True)
])
df2 = spark.read.option("header",True).schema(schema).csv(cleanpath)
#df_tracks.show()
df2 = df2.repartition(8)

# Update to your GCS bucket
gcs_bucket = gcs_bucket_name

# Update to your BigQuery dataset name you created
bq_dataset = 'bq_project_dezoomcamp'

# Enter BigQuery table name you want to create or overwite. 
# If the table does not exist it will be created when you run the write function
bq_table = 'fact_most_tracks'

df2.write \
.format("bigquery") \
.option("table","{}.{}".format(bq_dataset, bq_table)) \
.option("temporaryGcsBucket", gcs_bucket) \
.mode('overwrite') \
.save()

