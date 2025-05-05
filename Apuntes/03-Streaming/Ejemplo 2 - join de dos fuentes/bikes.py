from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp, window
from pyspark.sql.types import StructType, StringType, DoubleType, IntegerType

# Creamos a sesión de Spark
spark = SparkSession.builder \
    .appName("BikeStatusByCity") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Definimos o esquema dos datos do Kafka topic "bikes-status"
schema = StructType() \
    .add("station_id", StringType()) \
    .add("name", StringType()) \
    .add("city", StringType()) \
    .add("latitude", DoubleType()) \
    .add("longitude", DoubleType()) \
    .add("free_bikes", IntegerType()) \
    .add("empty_slots", IntegerType()) \
    .add("timestamp", StringType())

# Lemos o fluxo de Kafka
bikes_stream = spark.readStream \
    .format("kafka") \
    .option("subscribe", "bikes-status") \
    .option("kafka.bootstrap.servers", "kafka-1:9092") \
    .load()

# Parseamos os datos JSON e engadimos o campo de tempo
bikes_parsed = bikes_stream \
    .selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*") \
    .withColumn("event_time", to_timestamp("timestamp"))

# Creamos ventás de 10 minutos para agregación
agg = bikes_parsed \
    .withWatermark("event_time", "5 minutes") \
    .groupBy(
        window(col("event_time"), "10 minutes"),
        col("city")
    ) \
    .sum("free_bikes", "empty_slots")

# Amosamos a saída por consola
query = agg.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", False) \
    .start()

query.awaitTermination()
