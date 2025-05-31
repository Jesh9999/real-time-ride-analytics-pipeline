from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("KafkaCheckDebug").getOrCreate()

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "ride_data") \
    .option("startingOffsets", "earliest") \
    .load()

df.printSchema()

df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)").writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .start() \
    .awaitTermination()
