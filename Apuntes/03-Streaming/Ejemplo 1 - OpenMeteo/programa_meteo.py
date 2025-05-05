from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp, window, avg
from pyspark.sql.types import StructType, DoubleType, IntegerType, StringType

# Crear SparkSession
spark = SparkSession.builder \
    .appName("OpenMeteoWindowedToParquet") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "2") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Definir esquema
schema = StructType() \
    .add("temperature", DoubleType()) \
    .add("windspeed", DoubleType()) \
    .add("winddirection", DoubleType()) \
    .add("weathercode", IntegerType()) \
    .add("time", StringType()) \
    .add("city", StringType()) \
    .add("local_timestamp", StringType())

# Leer desde Kafka
df_kafka = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka-1:9092") \
    .option("subscribe", "open-meteo-weather") \
    .option("startingOffsets", "latest") \
    .load()

# Parseo
df_parsed = df_kafka.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select(
        col("data.city"),
        col("data.temperature"),
        col("data.windspeed"),
        to_timestamp(col("data.local_timestamp")).alias("event_time")
    )

# Agrupación por ventana y ciudad
df_grouped = df_parsed \
    .withWatermark("event_time", "2 minutes") \
    .groupBy(
        window(col("event_time"), "1 minute"),
        col("city")
    ).agg(
        avg("temperature").alias("avg_temp"),
        avg("windspeed").alias("avg_wind")
    )

# Mostrar en consola
query = df_grouped.select(
    col("window.start").alias("window_start"),
    col("window.end").alias("window_end"),
    "city",
    "avg_temp",
    "avg_wind"
).writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query.awaitTermination()

