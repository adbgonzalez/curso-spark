from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp, date_trunc, window, sum, avg
from pyspark.sql.types import StructType, StringType, DoubleType

# Spark session
spark = SparkSession.builder \
    .appName("MeteoCryptoJoinRounded") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Schemas
weather_schema = StructType() \
    .add("temperature", DoubleType()) \
    .add("windspeed", DoubleType()) \
    .add("weathercode", DoubleType()) \
    .add("city", StringType()) \
    .add("timestamp", StringType())

crypto_schema = StructType() \
    .add("coin", StringType()) \
    .add("usd", DoubleType()) \
    .add("eur", DoubleType()) \
    .add("timestamp", StringType())



# Stream datos meteorolóxicos
weather = spark.readStream \
    .format("kafka") \
    .option("subscribe", "open-meteo-weather") \
    .option("kafka.bootstrap.servers", "kafka-1:9092") \
    .option("startingOffsets", "earliest") \
    .load() \
    .selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), weather_schema).alias("data")) \
    .selectExpr("data.temperature", "data.windspeed", "data.timestamp as ts") \
    .withColumn("event_time", to_timestamp("ts"))

weather_agg = weather \
    .withWatermark("event_time", "5 minutes") \
    .groupBy(window(col("event_time"), "2 minutes")) \
    .avg("temperature", "windspeed") \
    .withColumnRenamed("avg(temperature)", "avg_temp") \
    .withColumnRenamed("avg(windspeed)", "avg_wind")


# Stream cripto
crypto = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka-1:9092") \
    .option("subscribe", "crypto-prices") \
    .load() \
    .selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), crypto_schema).alias("data")) \
    .selectExpr("data.coin", "data.usd", "data.eur", "data.timestamp as ts") \
    .withColumn("event_time", to_timestamp("ts")) \



crypto_agg = crypto\
    .withWatermark("event_time","5 minutes") \
    .groupBy(window(col("event_time"), "2 minutes")) \
    .avg("usd", "eur")\
    .withColumnRenamed("avg(usd)","avg_usd") \
    .withColumnRenamed("avg(eur)","avg_eur")

# Join por window
joined = weather_agg.join(crypto_agg, on="window")

# Saída
query = joined.select(
    col("window"),
    col("avg_temp"),
    col("avg_wind"),
    col("avg_usd"),
    col("avg_eur")
).writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .start()

query.awaitTermination()
