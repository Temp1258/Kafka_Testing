# Kafka_Testing
Kafka Practices including:  1. Kafka + Python + PostgreSQL streaming pipeline: reads CSV, streams to Kafka, stores in PostgreSQL (aggregated &amp; raw). 2. Real-time CDC pipeline using Kafka and Python: tracks PostgreSQL changes and syncs them to a target table via Kafka.

# Kafka + PostgreSQL Streaming Pipeline Project

This project demonstrates a complete data streaming pipeline using **Kafka**, **Python**, and **PostgreSQL**, from ingestion to processing and storage. It was developed as part of a Data Engineering hands-on assignment.

---

## 📦 Project Overview
- **Input**: `Employee_Salaries.csv` - Raw salary data file
- **Producer**: Filters and sends data to Kafka topic `employee_salaries`
- **Consumer**: Reads from Kafka and writes to PostgreSQL:
  - `department_employee_salary`: Aggregated total salary by department
  - `employee_B`: Raw records with department and salary

---

## 🔧 Technologies Used
- **Apache Kafka** (`confluentinc/cp-kafka` Docker image)
- **Zookeeper** (`confluentinc/cp-zookeeper` Docker image)
- **PostgreSQL** (`postgres:14-alpine` Docker image)
- **Python 3.13** with `confluent-kafka`, `psycopg2`, `pandas`
- **Docker Desktop** for container orchestration
- **DBeaver** for PostgreSQL GUI

---

## 🗂️ Project Structure
```text
├── docker-compose.yml           # Kafka + Zookeeper + PostgreSQL setup
├── producer.py                  # Reads CSV, filters, sends to Kafka
├── consumer.py                  # Consumes Kafka messages and stores in Postgres
├── employee.py                  # Defines Employee class with serialization
├── Employee_Salaries.csv        # Input CSV file
├── requirements.txt             # Python dependencies
```

---

## 🚀 How It Works
### 🔁 Producer Workflow (`producer.py`)
1. Reads and filters CSV rows:
   - Keeps only `ECC`, `CIT`, `EMS` departments
   - Filters out employees hired before 2010
   - Rounds down salary to integer
2. Sends messages to Kafka topic `employee_salaries`

### 🧾 Consumer Workflow (`consumer.py`)
1. Reads messages from topic `employee_salaries`
2. Parses employee JSON records
3. Updates PostgreSQL:
   - Inserts or updates `department_employee_salary` (sum of salaries per dept)
   - Appends raw data to `employee_B`

---

## 🐳 Running the Project (Quick Guide)

### 1. Start Docker Containers
```bash
docker-compose up -d
```

### 2. Run Producer
```bash
python producer.py
```

### 3. Run Consumer
```bash
python consumer.py
```

### 4. View Results in PostgreSQL (via DBeaver)
- Host: `localhost`
- Port: `5432`
- Database: `postgres`
- User: `postgres`
- Password: `postgres`
- Tables:
  - `department_employee_salary`
  - `employee_B`

---

## 🧪 Sample Output
Example query:
```sql
SELECT * FROM department_employee_salary;
```
| department | total_salary |
|------------|--------------|
| ECC        | 4281332      |
| CIT        | 13869807     |
| EMS        | 6127454      |

---

## ✍️ Author
- Diwen Liu (Project by BeaconFire Training)

---

## 📄 License
MIT License - feel free to reuse and adapt!


