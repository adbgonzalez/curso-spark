from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp
from pyspark.sql.types import StructType, DoubleType, IntegerType, StringType

# Crear sesión Spark con menos logs molestos
spark = SparkSession.builder \
    .appName("OpenMeteoStreamingClean") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "2") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .getOrCreate()

# Reducir nivel de logs
spark.sparkContext.setLogLevel("ERROR")

# Definir esquema extendido
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
        col("data.winddirection"),
        to_timestamp(col("data.local_timestamp")).alias("event_time")
    )

# Mostrar en consola sin datos de control
query = df_parsed.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .start()

query.awaitTermination()
