#!/home/nivag/2023-Linux/.python-3.10/bin/python

# by @mohgavin
import os
import sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

from pyspark.sql import SparkSession
from sedona.spark import *
from pyspark.sql.functions import col, concat_ws, expr
from sedona.register import SedonaRegistrator

spark = SparkSession \
    .builder.master("local") \
    .appName("MDT Process by Pyspark - Sedona") \
    .config("spark.driver.bindAddress","localhost") \
    .config("spark.ui.port","8787") \
    .getOrCreate()

sedona = SedonaContext.builder() .\
    config('spark.jars.packages',
           'org.apache.sedona:sedona-spark-shaded-3.5_2.12:1.5.0,'
           'org.datasyslab:geotools-wrapper:1.5.0-28.2'). \
    getOrCreate()

sedona = SedonaContext.create(spark)

spark_sedona_df_mdt =  sedona.read.option("header", True).csv('/mnt/h/Compile-MDT')
spark_sedona_df_grid = sedona.read.option("header", True).csv('/home/nivag/2023-Linux/csv_polygon/inbuilding.csv')

spark_sedona_df_mdt = spark_sedona_df_mdt.select(col("site"), col("ci"), col("longitude"), col("latitude"), col("rsrp_serving"))

# spark_sedona_df_mdt = spark_sedona_df_mdt.select('site', 'ci', 
#                                                 concat_ws(',', spark_sedona_df_mdt['longitude'], spark_sedona_df_mdt['latitude']).alias('Coordinates'), 'rsrp_serving')

spark_sedona_df_mdt = spark_sedona_df_mdt.withColumn("geometry", expr("st_point(longitude, latitude)"))

spark_sedona_df_mdt.show()
