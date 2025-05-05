from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp, window
from pyspark.sql.types import StructType, StringType, DoubleType, IntegerType

# Inicializamos Spark session
spark = SparkSession.builder \
    .appName("BikesWeather_Join_Barcelona") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Creamos o Schema para as bicis
bikes_schema = StructType() \
    .add("station_id", StringType()) \
    .add("name", StringType()) \
    .add("city", StringType()) \
    .add("latitude", DoubleType()) \
    .add("longitude", DoubleType()) \
    .add("free_bikes", IntegerType()) \
    .add("empty_slots", IntegerType()) \
    .add("timestamp", StringType())

# Creamos o Schema para datos meteorolóxicos
weather_schema = StructType() \
    .add("city", StringType()) \
    .add("temperature", DoubleType()) \
    .add("windspeed", DoubleType()) \
    .add("timestamp", StringType())

# Iniciamos o Stream para as bicis.
bikes = spark.readStream \
    .format("kafka") \
    .option("subscribe", "bikes-status") \
    .option("kafka.bootstrap.servers", "kafka-1:9092") \
    .option("startingOffsets", "earliest") \
    .load() \
    .selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), bikes_schema).alias("data")) \
    .filter(col("data.city") == "Barcelona") \
    .selectExpr("data.free_bikes", "data.empty_slots", "data.timestamp as ts") \
    .withColumn("event_time", to_timestamp("ts"))

# Agrupamos por ventá de event_time cada 2 minutos sumando as total_bikes e as total_empty. Watermark de 5 minutos
bikes_agg = bikes \
    .withWatermark("event_time", "5 minutes") \
    .groupBy(window(col("event_time"), "2 minutes")) \
    .sum("free_bikes", "empty_slots") \
    .withColumnRenamed("sum(free_bikes)", "total_bikes") \
    .withColumnRenamed("sum(empty_slots)", "total_empty")

# WriteStream de proba:
bikes_agg.writeStream \
    .format("console") \
    .outputMode("append") \
    .option("truncate", False) \
    .option("checkpointLocation", "/user/jovyan/bikes-check") \
    .start()


# Creamos o stream de lectura para os datos meteorolóxicos
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

# Agregamos os datos meteorolóxicos por ventá de event_time calculando a media da temperatura e da velocidade do tempo. Watermark de 5 minutos
weather_agg = weather \
    .withWatermark("event_time", "10 minutes") \
    .groupBy(window(col("event_time"), "2 minutes")) \
    .avg("temperature", "windspeed") \
    .withColumnRenamed("avg(temperature)", "avg_temp") \
    .withColumnRenamed("avg(windspeed)", "avg_wind")

#Writestream de proba:
weather_agg.writeStream \
    .format("console") \
    .outputMode("append") \
    .option("truncate", False) \
    .option("checkpointLocation", "/user/jovyan/weather-check") \
    .start()

# 🔗 Join por 'window'
joined = bikes_agg.join(weather_agg, on="window")

# Aaída en modo append a consola
query = joined.select(
    col("window"),
    "total_bikes", "total_empty", "avg_temp", "avg_wind"
).writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .option("checkpointLocation", "/user/jovyan/spark-checkpoint/bikes-weather-barcelona-v3") \
    .start()

query.awaitTermination()
