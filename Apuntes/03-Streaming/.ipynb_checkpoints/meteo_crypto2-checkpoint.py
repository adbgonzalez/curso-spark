from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp, date_trunc
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

# Stream meteo
weather = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka-1:9092") \
    .option("subscribe", "open-meteo-weather") \
    .load() \
    .selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), weather_schema).alias("data")) \
    .selectExpr("data.city", "data.temperature", "data.windspeed", "data.timestamp as ts") \
    .withColumn("event_time", to_timestamp("ts")) \
    .withColumn("minute_bucket", date_trunc("minute", col("event_time")))

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
    .withColumn("minute_bucket", date_trunc("minute", col("event_time")))
# Añadimos watermark para que nos permita agrupar:
# Creamos o bucket
weather = weather \
    .withColumn("minute_bucket", date_trunc("minute", col("event_time"))) \
    .withWatermark("minute_bucket", "2 minutes")

crypto = crypto \
    .withColumn("minute_bucket", date_trunc("minute", col("event_time"))) \
    .withWatermark("minute_bucket", "2 minutes")


# Agregación por tempo e city/coin
weather_agg = weather.groupBy("minute_bucket", "city") \
    .avg("temperature", "windspeed")

crypto_agg = crypto.groupBy("minute_bucket", "coin") \
    .avg("usd", "eur")

# Join por "minute_bucket"
joined = weather_agg.join(crypto_agg, on="minute_bucket")

# Saída
query = joined.select(
    col("minute_bucket").alias("window_start"),
    "city", "coin",
    col("avg(temperature)").alias("avg_temp"),
    col("avg(windspeed)").alias("avg_wind"),
    col("avg(usd)").alias("avg_usd"),
    col("avg(eur)").alias("avg_eur")
).writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .start()

query.awaitTermination()
