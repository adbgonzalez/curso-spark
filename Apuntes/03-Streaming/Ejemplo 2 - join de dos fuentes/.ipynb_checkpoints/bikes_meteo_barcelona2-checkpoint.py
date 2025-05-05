from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp, window, avg
from pyspark.sql.types import StructType, StringType, DoubleType, IntegerType

# Inicializamos spark session
spark = SparkSession.builder \
    .appName("BikesWeather_Join_Barcelona_Medias") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Schema para as bicis
bikes_schema = StructType() \
    .add("station_id", StringType()) \
    .add("name", StringType()) \
    .add("city", StringType()) \
    .add("latitude", DoubleType()) \
    .add("longitude", DoubleType()) \
    .add("free_bikes", IntegerType()) \
    .add("empty_slots", IntegerType()) \
    .add("timestamp", StringType())

#Schema para os datos meteorolóxicos:
weather_schema = StructType() \
    .add("city", StringType()) \
    .add("temperature", DoubleType()) \
    .add("windspeed", DoubleType()) \
    .add("timestamp", StringType())


# Iniciamos o readstream para as bicis
bikes = spark.readStream \
    .format("kafka") \
    .option("subscribe", "bikes-status") \
    .option("failOnDataLoss", "false") \
    .option("kafka.bootstrap.servers", "kafka-1:9092") \
    .option("startingOffsets", "latest") \
    .load() \
    .selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), bikes_schema).alias("data")) \
    .filter(col("data.city") == "Barcelona") \
    .selectExpr("data.station_id", "data.free_bikes", "data.empty_slots", "data.timestamp as ts") \
    .withColumn("event_time", to_timestamp("ts"))

# Calculamos a media por estación por ventá
bikes_avg_per_station = bikes \
    .withWatermark("event_time", "10 minutes") \
    .groupBy(
        window(col("event_time"), "2 minutes"),
        col("station_id")
    ) \
    .avg("free_bikes", "empty_slots") \
    .withColumnRenamed("avg(free_bikes)", "avg_bikes_station") \
    .withColumnRenamed("avg(empty_slots)", "avg_empty_station")

# Calculamos a media global
bikes_avg_global = bikes_avg_per_station \
    .groupBy("window") \
    .avg("avg_bikes_station", "avg_empty_station") \
    .withColumnRenamed("avg(avg_bikes_station)", "avg_bikes") \
    .withColumnRenamed("avg(avg_empty_station)", "avg_empty")

# Stream datos meteorolóxicos
weather = spark.readStream \
    .format("kafka") \
    .option("subscribe", "open-meteo-weather") \
    .option("kafka.bootstrap.servers", "kafka-1:9092") \
    .option("startingOffsets", "earliest") \
    .load() \
    .selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), weather_schema).alias("data")) \
    .filter(col("data.city") == "Barcelona") \
    .selectExpr("data.temperature", "data.windspeed", "data.timestamp as ts") \
    .withColumn("event_time", to_timestamp("ts"))

weather_agg = weather \
    .withWatermark("event_time", "10 minutes") \
    .groupBy(window(col("event_time"), "2 minutes")) \
    .avg("temperature", "windspeed") \
    .withColumnRenamed("avg(temperature)", "avg_temp") \
    .withColumnRenamed("avg(windspeed)", "avg_wind")

# 🔗 Join por 'window'
joined = bikes_avg_global.join(weather_agg, on="window")
# Saída por consola
joined.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .option("checkpointLocation", "/user/jovyan/checkpoint/bikes-avg-global") \
    .start() \
    .awaitTermination()
