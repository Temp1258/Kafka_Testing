# Kafka_Testing
Kafka Practices including:  1. Kafka + Python + PostgreSQL streaming pipeline: reads CSV, streams to Kafka, stores in PostgreSQL (aggregated &amp; raw). 2. Real-time CDC pipeline using Kafka and Python: tracks PostgreSQL changes and syncs them to a target table via Kafka.


🔧 Kafka-PostgreSQL Streaming Pipeline Project
This project demonstrates a complete data streaming pipeline using Kafka, Python, and PostgreSQL.

It reads salary data from a CSV file using a Kafka producer, streams it into a Kafka topic, then consumes the data using a Kafka consumer, and stores the aggregated or raw results into PostgreSQL tables.

📁 Project Structure
producer.py: Reads and filters employee salary data from CSV, sends messages to Kafka topic.

consumer.py: Listens to Kafka topic and inserts/aggregates data into PostgreSQL.

employee.py: Defines the Employee class and JSON serialization logic.

docker-compose.yml: Sets up Kafka, Zookeeper, and PostgreSQL containers.

Employee_Salaries.csv: Raw data source for streaming.

requirements.txt: Python dependencies.

🚀 Technologies Used
Apache Kafka (via confluent-kafka)

PostgreSQL

Python 3.11+

Docker & Docker Compose

DBeaver (for PostgreSQL GUI inspection)

✅ Features
Real-time streaming from CSV to Kafka to PostgreSQL

Data filtering and transformation (e.g., rounding down salaries, filtering by department/year)

Aggregated salary totals per department

Dual table storage: department_employee_salary (aggregated), employee_B (raw)
