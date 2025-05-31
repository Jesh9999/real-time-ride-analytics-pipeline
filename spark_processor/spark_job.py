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
    .option("startingOffsets", "earliest") \
    .load()
print("✅ Kafka stream read initialized")

# Parse JSON value from Kafka
df_json = df_raw.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# Convert timestamp field to TimestampType
df_final = df_json.withColumn("timestamp", col("timestamp").cast(TimestampType()))

# Write each batch to PostgreSQL
def write_to_postgres(batch_df, batch_id):
    try:
        batch_df.write \
            .format("jdbc") \
            .option("url", "jdbc:postgresql://localhost:5432/postgres") \
            .option("dbtable", "ride_data") \
            .option("user", "postgres") \
            .option("password", "Jesh9999$") \
            .option("driver", "org.postgresql.Driver") \
            .mode("append") \
            .save()
        print(f"✅ Batch {batch_id} written to PostgreSQL")
    except Exception as e:
        print(f"❌ Error writing batch {batch_id}: {e}")
        
query = df_final.writeStream \
    .foreachBatch(write_to_postgres) \
    .outputMode("append") \
    .start()

print("✅ Spark streaming job started and writing to PostgreSQL...")

query.awaitTermination()
