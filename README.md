# Real-time Ride Analytics Pipeline 

This project simulates and analyzes ride-hailing data in real-time using a full data engineering pipeline built with Kafka, Spark Structured Streaming, and PostgreSQL.

## Tech Stack

- **Apache Kafka** – real-time data ingestion
- **Apache Spark (Structured Streaming)** – data processing engine
- **PostgreSQL** – persistent data storage
- **Python** – core language used
- **Docker (optional)** – for containerized setup

## Features

- Simulates ride events (pickup, dropoff, distance, fare, timestamp)
- Streams events through Kafka topics
- Processes data in real-time using Spark
- Writes clean structured output to a PostgreSQL table
- Supports console display for debugging and testing

## 📂 Project Structure

```
Real-time-ride-share-Project/
│
├── kafka_producer/           # Python script to produce mock ride data
│   └── producer.py
│
├── spark_processor/          # Spark job for streaming processing
│   └── spark_job.py
│
├── data_simulator/           # Optional: another data simulation entry point
│   └── producer.py
│
├── kafka_test.py             # Kafka connection testing script
├── spark_job.py              # (possibly duplicate) Spark job script
```

## How to Run

### 1. Start Kafka and Zookeeper

```bash
bin/zookeeper-server-start.sh config/zookeeper.properties
bin/kafka-server-start.sh config/server.properties
```

### 2. Create Kafka Topic

```bash
kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic ride_data
```

### 3. Run the Kafka Producer

```bash
python3 kafka_producer/producer.py
```

### 4. Run the Spark Streaming Job

```bash
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.2.5 \
  spark_processor/spark_job.py
```

### 5. Verify in PostgreSQL

```sql
SELECT * FROM ride_data ORDER BY timestamp DESC LIMIT 5;
```

## Status

✅ Kafka topic created  
✅ Producer sending real-time ride events  
✅ Spark job writing structured data to PostgreSQL  
✅ PostgreSQL table populated correctly  

## Author

**Jeswanth Adari**  
Data Engineer & Analyst
