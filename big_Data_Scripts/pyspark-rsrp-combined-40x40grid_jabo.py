#!/home/nivag/2023-Linux/.python-3.10/bin/python

# by @mohgavin
from pyspark.sql import SparkSession
from sedona.register import SedonaRegistrator
from sedona.utils import SedonaKryoRegistrator, SedonaConf
from pyspark.sql.functions import col, concat_ws
from pyspark.sql.types import StringType

if __name__ == '__main__':
    # Initialize Spark session
    spark = SparkSession.builder.appName("SedonaExample").getOrCreate()

    # Register Sedona
    SedonaRegistrator.registerAll(spark)
    SedonaKryoRegistrator.registerKryoClasses(spark)

    # Set Sedona configuration
    SedonaConf.addVizServerExecutors(spark._sc)
    SedonaConf.addVizServerDependency(spark._sc)

    print('Loading Files...')
    mdt_df = spark.read.csv('Compile-MDT/mdt*.csv', header=True, inferSchema=True)
    grid_df = spark.read.parquet('grid_folder/40x40grid_alljabo_filtered.parquet')

    # Create Sedona geometries
    mdt_df = mdt_df.withColumn("pointer", concat_ws(" ", col("longitude").cast(StringType()), col("latitude").cast(StringType())))
    mdt_df.createOrReplaceTempView("mdt_df")
    spark.sql("SELECT *, ST_PointFromText(pointer, ' ') AS geom FROM mdt_df").createOrReplaceTempView("mdt_df")

    grid_df = grid_df.withColumn("geometry_polygon", col("geometry").cast(StringType()))
    grid_df.createOrReplaceTempView("grid_df")
    spark.sql("SELECT *, ST_GeomFromWKT(geometry_polygon) AS geom FROM grid_df").createOrReplaceTempView("grid_df")

    print('Join Operation...')
    spark.sql("SELECT m.*, g.* FROM mdt_df AS m, grid_df AS g WHERE ST_Intersects(m.geom, g.geom)").createOrReplaceTempView("joined_df")

    # Perform the necessary operations to create pivot tables
    pivot_mean = spark.sql("SELECT geometry_polygon, rsrp_combined, mean(rsrp_serving) AS mean_rsrp_serving FROM joined_df GROUP BY geometry_polygon, rsrp_combined")
    pivot_count = spark.sql("SELECT geometry_polygon, rsrp_combined, count(rsrp_serving) AS count_rsrp_serving FROM joined_df GROUP BY geometry_polygon, rsrp_combined")

    print('Saving Files...')
    pivot_mean.coalesce(1).write.csv('result/rsrp-combined-alljabo-40x40.csv', header=True)
    pivot_count.coalesce(1).write.csv('result/rsrp-combined-alljabo-40x40-pop.csv', header=True)

    print('Finished...')

    spark.stop()
