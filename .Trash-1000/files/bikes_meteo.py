from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp, date_trunc
from pyspark.sql.types import StructType, StringType, DoubleType, IntegerType

# Spark session
spark = SparkSession.builder \
    .appName("BikesWeatherJoin") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Schemas
bikes_schema = StructType() \
    .add("station_id", StringType()) \
    .add("name", StringType()) \
    .add("city", StringType()) \
    .add("latitude", DoubleType()) \
    .add("longitude", DoubleType()) \
    .add("free_bikes", IntegerType()) \
    .add("empty_slots", IntegerType()) \
    .add("timestamp", StringType())

weather_schema = StructType() \
    .add("city", StringType()) \
    .add("temperature", DoubleType()) \
    .add("windspeed", DoubleType()) \
    .add("timestamp", StringType())


"""
# Bikes
bikes = spark.readStream \
    .format("kafka") \
    .option("subscribe", "bikes-status") \
    .option("kafka.bootstrap.servers", "kafka-1:9092") \
    .load() \
    .selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), bikes_schema).alias("data")) \
    .select("data.*") \
    .withColumn("event_time", to_timestamp("timestamp")) \
    .withWatermark("event_time", "2 minutes")

bikes_agg = bikes \
    .withColumn("minute_bucket", date_trunc("minute", col("event_time"))) \
    .groupBy("minute_bucket", "city") \
    .sum("free_bikes", "empty_slots") \
    .withColumnRenamed("sum(free_bikes)", "total_bikes") \
    .withColumnRenamed("sum(empty_slots)", "total_empty")

# Weather
weather = spark.readStream \
    .format("kafka") \
    .option("subscribe", "open-meteo-weather") \
    .option("kafka.bootstrap.servers", "kafka-1:9092") \
    .load() \
    .selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), weather_schema).alias("data")) \
    .select("data.*") \
    .withColumn("event_time", to_timestamp("timestamp")) \
    .withWatermark("event_time", "2 minutes")

weather_agg = weather \
    .withColumn("minute_bucket", date_trunc("minute", col("event_time"))) \
    .groupBy("minute_bucket", "city") \
    .avg("temperature", "windspeed") \
    .withColumnRenamed("avg(temperature)", "avg_temp") \
    .withColumnRenamed("avg(windspeed)", "avg_wind")




"""
# Stream de bikes
bikes = spark.readStream \
    .format("kafka") \
    .option("subscribe", "bikes-status") \
    .option("kafka.bootstrap.servers", "kafka-1:9092") \
    .load() \
    .selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), bikes_schema).alias("data")) \
    .select("data.*") \
    .withColumn("event_time", to_timestamp("timestamp")) \
    .withWatermark("event_time", "2 minutes") \
    .withColumn("minute_bucket", date_trunc("minute", col("event_time")))

bikes_agg = bikes.groupBy("minute_bucket", "city") \
    .sum("free_bikes", "empty_slots") \
    .withColumnRenamed("sum(free_bikes)", "total_bikes") \
    .withColumnRenamed("sum(empty_slots)", "total_empty")

# Stream de tempo
weather = spark.readStream \
    .format("kafka") \
    .option("subscribe", "open-meteo-weather") \
    .option("kafka.bootstrap.servers", "kafka-1:9092") \
    .load() \
    .selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), weather_schema).alias("data")) \
    .select("data.*") \
    .withColumn("event_time", to_timestamp("timestamp")) \
    .withWatermark("event_time", "2 minutes") \
    .withColumn("minute_bucket", date_trunc("minute", col("event_time")))

weather_agg = weather.groupBy("minute_bucket", "city") \
    .avg("temperature", "windspeed") \
    .withColumnRenamed("avg(temperature)", "avg_temp") \
    .withColumnRenamed("avg(windspeed)", "avg_wind")

# JOIN por city + minuto
bikes_agg = bikes_agg.withWatermark("minute_bucket", "2 minutes")
weather_agg = weather_agg.withWatermark("minute_bucket", "2 minutes")


joined = bikes_agg.join(weather_agg, on=["minute_bucket", "city"])

# Saída por consola
query = joined.select(
    col("minute_bucket").alias("window"),
    "city",
    "total_bikes",
    "total_empty",
    "avg_temp",
    "avg_wind"
).writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .start()
"""
joined = bikes_agg.join(weather_agg, on=["minute_bucket", "city"])

query = joined.select(
    col("minute_bucket").alias("window"),
    "city",
    "total_bikes",
    "total_empty",
    "avg_temp",
    "avg_wind"
).writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .start()

query.awaitTermination()

"""
query.awaitTermination()
