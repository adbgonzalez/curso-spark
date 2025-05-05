from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
from pyspark.sql.functions import expr
import time, random

spark = SparkSession.builder \
    .master("spark://spark-master:7077") \
    .appName("importe_total") \
    .config("spark.eventLog.enabled", "true") \
    .config("spark.eventLog.dir", "hdfs:///spark/logs/history") \
    .config("spark.history.fs.logDirectory", "hdfs:///spark/logs/history") \
    .getOrCreate()

sc = spark.sparkContext


# Definir el esquema
schema = StructType([
    StructField("InvoiceNo", IntegerType(), True),
    StructField("StockCode", StringType(), True),
    StructField("Description", StringType(), True),
    StructField("Quantity", IntegerType(), True),
    StructField("InvoiceDate", TimestampType(), True),
    StructField("UnitPrice", DoubleType(), True),
    StructField("CustomerID", IntegerType(), True),
    StructField("Country", StringType(), True)
])

# Leer el archivo CSV con el esquema especificado
print ("Antes de leer csv")
df = spark.read.csv("hdfs:///user/jovyan/data/online-retail-dataset.csv", header=True, schema=schema)
print ("despues de leer csv")
new_df = df.select("CustomerID", expr("Quantity * UnitPrice AS ImporteTotal")).groupBy("CustomerID").avg("ImporteTotal")
print ("Antes de escribir json")
new_df.write.mode("overwrite").json("hdfs:///user/jovyan/output/salida1")
print ("Despues de escribir json")

#time.sleep(100)
