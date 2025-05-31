from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, FloatType, TimestampType

# Define schema
schema = StructType() \
    .add("timestamp", StringType()) \
    .add("pickup", StringType()) \
    .add("dropoff", StringType()) \
    .add("distance_km", FloatType()) \
    .add("fare_usd", FloatType())

# Spark session
spark = SparkSession.builder \
    .appName("KafkaRideConsumer") \
    .getOrCreate()
print("✅ Spark session created")

# Read from Kafka
df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "ride_data") \
    .option("startingOffsets", "latest") \
    .load()
print("✅ Kafka stream read initialized")

# Parse JSON value from Kafka
df_json = df_raw.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# Convert timestamp field to TimestampType
df_final = df_json.withColumn("timestamp", col("timestamp").cast(TimestampType()))

# Write to console for debugging
query = df_final.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .start()

print("✅ Spark streaming job started — waiting for Kafka messages...")

query.awaitTermination()
