from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType, DateType
from pyspark.sql.functions import expr,count
import time, random


spark = SparkSession.builder \
    .master("spark://spark-master:7077") \
    .appName("estaciones_viajes") \
    .config("spark.eventLog.enabled", "true") \
    .config("spark.eventLog.dir", "hdfs:///spark/logs/history") \
    .config("spark.history.fs.logDirectory", "hdfs:///spark/logs/history") \
    .getOrCreate()

sc = spark.sparkContext


# Definir el esquemas

station_schema = StructType([
    StructField("station_id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("lat", DoubleType(), True),
    StructField("long", DoubleType(), True),
    StructField("dockcount", IntegerType(), True),
    StructField("landmark", StringType(), True),
    StructField("installation", DateType(), True)
])

trip_schema = StructType([
   StructField("Trip Id", IntegerType(), True),
   StructField("Duration", IntegerType(), True),
   StructField("Start Date", DateType(), True),
   StructField("Start Station", StringType(), True),
   StructField("Start Terminal", IntegerType(), True),
   StructField("End Date", DateType(), True),
   StructField("End Station", StringType(), True),
   StructField("End Terminal", IntegerType(), True),
   StructField("Bike", IntegerType(), True),
   StructField("Subscriber Type", StringType(), True),
   StructField("Zip Code", StringType(), True)
])
# Leer el archivo CSV con el esquema especificado
spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

stations_df = spark.read.csv("/user/jovyan/data/bike-data/201508_station_data.csv", header=True, schema=station_schema,dateFormat="M/D/yyyy")
trips_df = spark.read.csv("/user/jovyan/data/bike-data/201508_trip_data.csv", header=True, schema=trip_schema,dateFormat="M/D/yyyy")

# Contar cuántos viajes empiezan en cada estación
start_counts = trips_df.groupBy("Start Station").agg(count("*").alias("Numero_viajes_empiezan"))

# Contar cuántos viajes terminan en cada estación
end_counts = trips_df.groupBy("End Station").agg(count("*").alias("Numero_viajes_acaban"))

aux_df = stations_df.join(start_counts, stations_df["name"] == start_counts["Start Station"], "left")

join_df = aux_df.join(end_counts, stations_df["name"] == end_counts["End Station"], "left")


result_df = join_df.selectExpr("name as Estacion", "lat as Latitud", "long as Longitud", "Numero_viajes_empiezan", "Numero_viajes_acaban")

result_df.write.mode("overwrite").csv("/user/jovyan/output/salida2", header=True)
time.sleep(100)
