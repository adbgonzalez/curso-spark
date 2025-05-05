rom pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp, window, avg
from pyspark.sql.types import StructType, StringType, DoubleType, IntegerType

# 🚀 Spark session
spark = SparkSession.builder \
    .appName("BikesWeather_Join_Barcelona_Medias") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# 📐 Schema de datos das bicis
bikes_schema = StructType() \
    .add("station_id", StringType()) \
    .add("name", StringType()) \
    .add("city", StringType()) \
    .add("latitude", DoubleType()) \
    .add("longitude", DoubleType()) \
    .add("free_bikes", IntegerType()) \
    .add("empty_slots", IntegerType()) \
    .add("timestamp", StringType())

# 🔄 Lectura do stream de Kafka (só datos de Barcelona)
bikes = spark.readStream \
    .format("kafka") \
    .option("subscribe", "bikes-status") \
    .option("kafka.bootstrap.servers", "kafka-1:9092") \
    .option("startingOffsets", "latest") \
    .load() \
    .selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), bikes_schema).alias("data")) \
    .filter(col("data.city") == "Barcelona") \
    .selectExpr("data.station_id", "data.free_bikes", "data.empty_slots", "data.timestamp as ts") \
    .withColumn("event_time", to_timestamp("ts"))

# 1️⃣ Media por estación por ventá
bikes_avg_per_station = bikes \
    .withWatermark("event_time", "10 minutes") \
    .groupBy(
        window(col("event_time"), "2 minutes"),
        col("station_id")
    ) \
    .avg("free_bikes", "empty_slots") \
    .withColumnRenamed("avg(free_bikes)", "avg_bikes_station") \
    .withColumnRenamed("avg(empty_slots)", "avg_empty_station")

# 2️⃣ Media global agregando os valores anteriores
bikes_avg_global = bikes_avg_per_station \
    .groupBy("window") \
    .avg("avg_bikes_station", "avg_empty_station") \
    .withColumnRenamed("avg(avg_bikes_station)", "avg_bikes") \
    .withColumnRenamed("avg(avg_empty_station)", "avg_empty")

# 💾 Saída por consola (podes adaptala a Kafka ou ficheiros se queres)
bikes_avg_global.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .option("checkpointLocation", "/user/jovyan/checkpoint/bikes-avg-global") \
    .start() \
    .awaitTermination()