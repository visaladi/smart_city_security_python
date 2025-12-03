# Smart City Traffic – Big Data Mini Project

Real-time smart city traffic monitoring pipeline using:

- **Kafka** – traffic sensor event stream (`traffic_raw` topic)  
- **Python producers/consumers** – simulate sensors + load into PostgreSQL  
- **Spark Structured Streaming** – 5-minute window aggregates + congestion index  
- **Airflow** – nightly batch job to compute peak hour  
- **PostgreSQL** – `traffic_readings` + `traffic_aggregates`  
- **FastAPI + HTML Dashboard** – REST APIs + live dashboard UI  
- **Grafana** – visual monitoring on top of PostgreSQL  

> Platform: **Windows 11 + PowerShell**

---

## 1. Project Structure (simplified)

# Smart City Traffic – Big Data Mini Project

Real-time smart city traffic monitoring pipeline using:

- **Kafka** – traffic sensor event stream (`traffic_raw` topic)  
- **Python producers/consumers** – simulate sensors + load into PostgreSQL  
- **Spark Structured Streaming** – 5-minute window aggregates + congestion index  
- **Airflow** – nightly batch job to compute peak hour  
- **PostgreSQL** – `traffic_readings` + `traffic_aggregates`  
- **FastAPI + HTML Dashboard** – REST APIs + live dashboard UI  
- **Grafana** – visual monitoring on top of PostgreSQL  

> Platform: **Windows 11 + PowerShell**

---

## 1. Project Structure (simplified)

```text
smart_city_security/
│
├─ docker-compose.yml
├─ dashboard_api.py         # FastAPI backend for the dashboard
├─ db_loader.py             # Kafka -> PostgreSQL loader (traffic_readings)
├─ traffic_producer.py      # Kafka producer – simulates sensor events
│
├─ spark/
│   └─ traffic_streaming.py # Spark Structured Streaming job
│
├─ airflow/
│   └─ dags/
│       └─ smart_city_traffic_batch_dag.py  # Airflow daily batch DAG
│
└─ static/
    └─ index.html           # Dashboard UI (Smart City Traffic Dashboard)
```
## 2. Prerequisites
### 2.1 Tools
Install / have:

Docker + Docker Compose

Python 3.x (e.g. C:\Python314\python.exe)

Java 17
Example:
C:\Users\visal\AppData\Local\Programs\Eclipse Adoptium\jdk-17.0.17.10-hotspot

Apache Spark 3.5.x for Windows
Example: C:\spark

Hadoop winutils for Windows

Create folder: C:\hadoop\bin

Put winutils.exe and hadoop.dll inside C:\hadoop\bin

## 3. PostgreSQL Setup
### 3.1 Start PostgreSQL
If PostgreSQL is in Docker:

powershell
Copy code
cd "C:\Users\visal\Desktop\uni sem 8\bigdata\final\smart_city_security"
docker-compose up -d
Make sure PostgreSQL is reachable at:

Host: localhost

Port: 5432

User: postgres

Password: 0956

DB: smart_city_traffic

### 3.2 Create Database & Tables
Connect using pgAdmin or psql:


CREATE DATABASE smart_city_traffic;
\c smart_city_traffic;

-- Raw events
CREATE TABLE IF NOT EXISTS traffic_readings (
    id              SERIAL PRIMARY KEY,
    sensor_id       VARCHAR(10) NOT NULL,
    event_time      TIMESTAMP   NOT NULL,
    vehicle_count   INT         NOT NULL,
    avg_speed       DOUBLE PRECISION NOT NULL
);

-- 5-minute window aggregates
CREATE TABLE IF NOT EXISTS traffic_aggregates (
    id                 SERIAL PRIMARY KEY,
    sensor_id          VARCHAR(10) NOT NULL,
    window_start       TIMESTAMP   NOT NULL,
    window_end         TIMESTAMP   NOT NULL,
    records            INT         NOT NULL,
    avg_vehicle_count  DOUBLE PRECISION NOT NULL,
    avg_speed          DOUBLE PRECISION NOT NULL,
    congestion_index   DOUBLE PRECISION NOT NULL
);
## 4. Environment Variables (Windows PowerShell)
In every Spark terminal you must set:


Copy code
### Hadoop home (must be UPPERCASE)
$env:HADOOP_HOME = "C:\hadoop"

### Java & Spark
$env:JAVA_HOME  = "C:\Users\visal\AppData\Local\Programs\Eclipse Adoptium\jdk-17.0.17.10-hotspot"
$env:SPARK_HOME = "C:\spark"

### Use a Python without spaces in the path (or your venv Python)
$env:PYSPARK_PYTHON = "C:\Python314\python.exe"

### Add to PATH
$env:Path = "$env:HADOOP_HOME\bin;$env:JAVA_HOME\bin;$env:SPARK_HOME\bin;$env:Path"
If you want to force Hadoop home inside Spark:

powershell
Copy code
# Extra (used in spark-submit)
# --conf "spark.driver.extraJavaOptions=-Dhadoop.home.dir=C:\hadoop"
# --conf "spark.executor.extraJavaOptions=-Dhadoop.home.dir=C:\hadoop"
## 5. How to Run – Multi-Terminal Setup
Always run from the project root:
C:\Users\visal\Desktop\uni sem 8\bigdata\final\smart_city_security

Terminal 1 – Start Docker (Kafka, Zookeeper, PostgreSQL, Kafdrop, Grafana)
powershell
Copy code
cd "C:\Users\visal\Desktop\uni sem 8\bigdata\final\smart_city_security"
docker-compose up -d
Check:

Kafka broker (e.g. kafka-smartcity:9092 / localhost:29092)

Kafdrop UI: http://localhost:19000 (or as per your docker-compose.yml)

Grafana UI: http://localhost:3000

Terminal 2 – (Optional) Activate Python venv
powershell
Copy code
cd "C:\Users\visal\Desktop\uni sem 8\bigdata\final\smart_city_security"
.\venv\Scripts\Activate.ps1
pip install -r requirements.txt   # if you have one
Terminal 3 – Run Kafka Producer (Sensor Simulator)
This script sends messages like:

text
Copy code
SENT: {'sensor_id': 'J1', 'timestamp': 1764646633, 'vehicle_count': 19, 'avg_speed': 22.5}
Run:

powershell
Copy code
cd "C:\Users\visal\Desktop\uni sem 8\bigdata\final\smart_city_security"
.\venv\Scripts\Activate.ps1   # optional
python traffic_producer.py
Make sure it is producing to the topic (e.g. traffic_raw).

Terminal 4 – Run DB Loader (Kafka → PostgreSQL)
This consumer stores raw events into traffic_readings:

powershell
Copy code
cd "C:\Users\visal\Desktop\uni sem 8\bigdata\final\smart_city_security"
.\venv\Scripts\Activate.ps1   # optional
python db_loader.py
You should see logs like:

text
Copy code
Received: {'sensor_id': 'J1', 'timestamp': ..., 'vehicle_count': 25, 'avg_speed': 47.05}
Terminal 5 – Run Spark Structured Streaming Job
This job:

Reads from Kafka

Does 5-minute windowing per sensor

Computes congestion_index

Writes into traffic_aggregates in PostgreSQL

powershell
Copy code
cd "C:\Users\visal\Desktop\uni sem 8\bigdata\final\smart_city_security"

# Make sure env vars are set (HADOOP_HOME, JAVA_HOME, SPARK_HOME, PYSPARK_PYTHON)
$env:HADOOP_HOME = "C:\hadoop"
$env:JAVA_HOME   = "C:\Users\visal\AppData\Local\Programs\Eclipse Adoptium\jdk-17.0.17.10-hotspot"
$env:SPARK_HOME  = "C:\spark"
$env:PYSPARK_PYTHON = "C:\Python314\python.exe"
$env:Path = "$env:HADOOP_HOME\bin;$env:JAVA_HOME\bin;$env:SPARK_HOME\bin;$env:Path"

& "C:\spark\bin\spark-submit.cmd" `
  --conf "spark.driver.extraJavaOptions=-Dhadoop.home.dir=C:\hadoop" `
  --conf "spark.executor.extraJavaOptions=-Dhadoop.home.dir=C:\hadoop" `
  --packages org.postgresql:postgresql:42.7.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.7 `
  .\spark\traffic_streaming.py
Open Spark UI:
http://localhost:4040

You should see 2 running streaming queries (raw & aggregates) without errors.

Terminal 6 – Run FastAPI Dashboard
This serves:

/api/raw/latest?limit=50 – last raw events

/api/aggregates/latest – latest window per sensor

/ – HTML dashboard (static/index.html)

powershell
Copy code
cd "C:\Users\visal\Desktop\uni sem 8\bigdata\final\smart_city_security"
.\venv\Scripts\Activate.ps1
uvicorn dashboard_api:app --reload --port 8000
Open the dashboard:
http://127.0.0.1:8000

You’ll see:

Left: Cards with 5-min congestion per sensor (index + severity badges)

Right: Table of latest raw events

Auto refresh every 5 seconds

6. Airflow – Nightly Batch Job (Peak Hour)
The DAG file is:
airflow/dags/smart_city_traffic_batch_dag.py

6.1 Initialize Airflow
From inside airflow/ (or where your AIRFLOW_HOME is):

powershell
Copy code
cd "C:\Users\visal\Desktop\uni sem 8\bigdata\final\smart_city_security\airflow"

# One-time init
airflow db init
airflow users create `
  --username admin `
  --password admin `
  --firstname Admin `
  --lastname User `
  --role Admin `
  --email admin@example.com
Copy smart_city_traffic_batch_dag.py into AIRFLOW_HOME/dags/ if not already there.

6.2 Run Airflow
Terminal A:

powershell
Copy code
airflow webserver
Terminal B:

powershell
Copy code
airflow scheduler
Open:
http://localhost:8080 → enable Smart City Traffic Batch DAG → it will:

Run daily

Read traffic_readings

Compute peak traffic hour per day

Write results into a daily summary table (as defined in the DAG)

7. Grafana Dashboards
Grafana usually runs via Docker at http://localhost:3000.

Basic steps:

Login (default): admin / admin (change password on first login)

Add a PostgreSQL data source:

Host: host.docker.internal:5432 (or postgres:5432 if inside Docker network)

Database: smart_city_traffic

User: postgres

Password: 0956

Create panels:

Panel 1 (Raw speed per sensor)
Query from traffic_readings (time series)

Panel 2 (Congestion index per sensor)
Query from traffic_aggregates

Panel 3 (Peak hour table)
Query from daily summary table created by Airflow

8. Order of Running (Quick Summary)
docker-compose up -d – Kafka, Zookeeper, Postgres, Kafdrop, Grafana

python traffic_producer.py – send sensor events → Kafka

python db_loader.py – consume Kafka → traffic_readings

spark-submit spark/traffic_streaming.py – streaming aggregates → traffic_aggregates

uvicorn dashboard_api:app --reload --port 8000 – dashboard + APIs

airflow webserver + airflow scheduler – nightly batch (peak hour)

Once all are running, you have:

Real-time streaming (Kafka + Spark + PostgreSQL)

Batch processing (Airflow nightly DAG)

API + UI (FastAPI + HTML dashboard)

Monitoring (Grafana)

9. Troubleshooting
NativeIO$Windows.access0 UnsatisfiedLinkError
→ Hadoop native libs not found.

Check C:\hadoop\bin\winutils.exe and hadoop.dll exist

Ensure HADOOP_HOME is set and on PATH

Use the extra Java options -Dhadoop.home.dir=C:\hadoop

Dashboard shows “Failed to load aggregates/raw”

Check:

Postgres is up

db_loader.py is writing into traffic_readings

traffic_streaming.py is running without errors

Kafka not receiving events

Check topic name in traffic_producer.py, db_loader.py, and Kafka config all match (e.g. traffic_raw
smart_city_security/
│
├─ docker-compose.yml
├─ dashboard_api.py         # FastAPI backend for the dashboard
├─ db_loader.py             # Kafka -> PostgreSQL loader (traffic_readings)
├─ traffic_producer.py      # Kafka producer – simulates sensor events
│
├─ spark/
│   └─ traffic_streaming.py # Spark Structured Streaming job
│
├─ airflow/
│   └─ dags/
│       └─ smart_city_traffic_batch_dag.py  # Airflow daily batch DAG
│
└─ static/
    └─ index.html           # Dashboard UI (Smart City Traffic Dashboard)
## 2. Prerequisites
2.1 Tools
Install / have:

Docker + Docker Compose

Python 3.x (e.g. C:\Python314\python.exe)

Java 17
Example:
C:\Users\visal\AppData\Local\Programs\Eclipse Adoptium\jdk-17.0.17.10-hotspot

Apache Spark 3.5.x for Windows
Example: C:\spark

Hadoop winutils for Windows

Create folder: C:\hadoop\bin

Put winutils.exe and hadoop.dll inside C:\hadoop\bin

3. PostgreSQL Setup
3.1 Start PostgreSQL
If PostgreSQL is in Docker:

powershell
Copy code
cd "C:\Users\visal\Desktop\uni sem 8\bigdata\final\smart_city_security"
docker-compose up -d
Make sure PostgreSQL is reachable at:

Host: localhost

Port: 5432

User: postgres

Password: 0956

DB: smart_city_traffic

3.2 Create Database & Tables
Connect using pgAdmin or psql:

sql
Copy code
CREATE DATABASE smart_city_traffic;
\c smart_city_traffic;

-- Raw events
CREATE TABLE IF NOT EXISTS traffic_readings (
    id              SERIAL PRIMARY KEY,
    sensor_id       VARCHAR(10) NOT NULL,
    event_time      TIMESTAMP   NOT NULL,
    vehicle_count   INT         NOT NULL,
    avg_speed       DOUBLE PRECISION NOT NULL
);

-- 5-minute window aggregates
CREATE TABLE IF NOT EXISTS traffic_aggregates (
    id                 SERIAL PRIMARY KEY,
    sensor_id          VARCHAR(10) NOT NULL,
    window_start       TIMESTAMP   NOT NULL,
    window_end         TIMESTAMP   NOT NULL,
    records            INT         NOT NULL,
    avg_vehicle_count  DOUBLE PRECISION NOT NULL,
    avg_speed          DOUBLE PRECISION NOT NULL,
    congestion_index   DOUBLE PRECISION NOT NULL
);
4. Environment Variables (Windows PowerShell)
In every Spark terminal you must set:

powershell
Copy code
# Hadoop home (must be UPPERCASE)
$env:HADOOP_HOME = "C:\hadoop"

# Java & Spark
$env:JAVA_HOME  = "C:\Users\visal\AppData\Local\Programs\Eclipse Adoptium\jdk-17.0.17.10-hotspot"
$env:SPARK_HOME = "C:\spark"

# Use a Python without spaces in the path (or your venv Python)
$env:PYSPARK_PYTHON = "C:\Python314\python.exe"

# Add to PATH
$env:Path = "$env:HADOOP_HOME\bin;$env:JAVA_HOME\bin;$env:SPARK_HOME\bin;$env:Path"
If you want to force Hadoop home inside Spark:

powershell
Copy code
# Extra (used in spark-submit)
# --conf "spark.driver.extraJavaOptions=-Dhadoop.home.dir=C:\hadoop"
# --conf "spark.executor.extraJavaOptions=-Dhadoop.home.dir=C:\hadoop"
5. How to Run – Multi-Terminal Setup
Always run from the project root:
C:\Users\visal\Desktop\uni sem 8\bigdata\final\smart_city_security

Terminal 1 – Start Docker (Kafka, Zookeeper, PostgreSQL, Kafdrop, Grafana)
powershell
Copy code
cd "C:\Users\visal\Desktop\uni sem 8\bigdata\final\smart_city_security"
docker-compose up -d
Check:

Kafka broker (e.g. kafka-smartcity:9092 / localhost:29092)

Kafdrop UI: http://localhost:19000 (or as per your docker-compose.yml)

Grafana UI: http://localhost:3000

Terminal 2 – (Optional) Activate Python venv
powershell
Copy code
cd "C:\Users\visal\Desktop\uni sem 8\bigdata\final\smart_city_security"
.\venv\Scripts\Activate.ps1
pip install -r requirements.txt   # if you have one
Terminal 3 – Run Kafka Producer (Sensor Simulator)
This script sends messages like:

text
Copy code
SENT: {'sensor_id': 'J1', 'timestamp': 1764646633, 'vehicle_count': 19, 'avg_speed': 22.5}
Run:

powershell
Copy code
cd "C:\Users\visal\Desktop\uni sem 8\bigdata\final\smart_city_security"
.\venv\Scripts\Activate.ps1   # optional
python traffic_producer.py
Make sure it is producing to the topic (e.g. traffic_raw).

Terminal 4 – Run DB Loader (Kafka → PostgreSQL)
This consumer stores raw events into traffic_readings:

powershell
Copy code
cd "C:\Users\visal\Desktop\uni sem 8\bigdata\final\smart_city_security"
.\venv\Scripts\Activate.ps1   # optional
python db_loader.py
You should see logs like:

text
Copy code
Received: {'sensor_id': 'J1', 'timestamp': ..., 'vehicle_count': 25, 'avg_speed': 47.05}
Terminal 5 – Run Spark Structured Streaming Job
This job:

Reads from Kafka

Does 5-minute windowing per sensor

Computes congestion_index

Writes into traffic_aggregates in PostgreSQL

powershell
Copy code
cd "C:\Users\visal\Desktop\uni sem 8\bigdata\final\smart_city_security"

# Make sure env vars are set (HADOOP_HOME, JAVA_HOME, SPARK_HOME, PYSPARK_PYTHON)
$env:HADOOP_HOME = "C:\hadoop"
$env:JAVA_HOME   = "C:\Users\visal\AppData\Local\Programs\Eclipse Adoptium\jdk-17.0.17.10-hotspot"
$env:SPARK_HOME  = "C:\spark"
$env:PYSPARK_PYTHON = "C:\Python314\python.exe"
$env:Path = "$env:HADOOP_HOME\bin;$env:JAVA_HOME\bin;$env:SPARK_HOME\bin;$env:Path"

& "C:\spark\bin\spark-submit.cmd" `
  --conf "spark.driver.extraJavaOptions=-Dhadoop.home.dir=C:\hadoop" `
  --conf "spark.executor.extraJavaOptions=-Dhadoop.home.dir=C:\hadoop" `
  --packages org.postgresql:postgresql:42.7.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.7 `
  .\spark\traffic_streaming.py
Open Spark UI:
http://localhost:4040

You should see 2 running streaming queries (raw & aggregates) without errors.

Terminal 6 – Run FastAPI Dashboard
This serves:

/api/raw/latest?limit=50 – last raw events

/api/aggregates/latest – latest window per sensor

/ – HTML dashboard (static/index.html)

powershell
Copy code
cd "C:\Users\visal\Desktop\uni sem 8\bigdata\final\smart_city_security"
.\venv\Scripts\Activate.ps1
uvicorn dashboard_api:app --reload --port 8000
Open the dashboard:
http://127.0.0.1:8000

You’ll see:

Left: Cards with 5-min congestion per sensor (index + severity badges)

Right: Table of latest raw events

Auto refresh every 5 seconds

6. Airflow – Nightly Batch Job (Peak Hour)
The DAG file is:
airflow/dags/smart_city_traffic_batch_dag.py

6.1 Initialize Airflow
From inside airflow/ (or where your AIRFLOW_HOME is):

powershell
Copy code
cd "C:\Users\visal\Desktop\uni sem 8\bigdata\final\smart_city_security\airflow"

# One-time init
airflow db init
airflow users create `
  --username admin `
  --password admin `
  --firstname Admin `
  --lastname User `
  --role Admin `
  --email admin@example.com
Copy smart_city_traffic_batch_dag.py into AIRFLOW_HOME/dags/ if not already there.

6.2 Run Airflow
Terminal A:

powershell
Copy code
airflow webserver
Terminal B:

powershell
Copy code
airflow scheduler
Open:
http://localhost:8080 → enable Smart City Traffic Batch DAG → it will:

Run daily

Read traffic_readings

Compute peak traffic hour per day

Write results into a daily summary table (as defined in the DAG)

7. Grafana Dashboards
Grafana usually runs via Docker at http://localhost:3000.

Basic steps:

Login (default): admin / admin (change password on first login)

Add a PostgreSQL data source:

Host: host.docker.internal:5432 (or postgres:5432 if inside Docker network)

Database: smart_city_traffic

User: postgres

Password: 0956

Create panels:

Panel 1 (Raw speed per sensor)
Query from traffic_readings (time series)

Panel 2 (Congestion index per sensor)
Query from traffic_aggregates

Panel 3 (Peak hour table)
Query from daily summary table created by Airflow

8. Order of Running (Quick Summary)
docker-compose up -d – Kafka, Zookeeper, Postgres, Kafdrop, Grafana

python traffic_producer.py – send sensor events → Kafka

python db_loader.py – consume Kafka → traffic_readings

spark-submit spark/traffic_streaming.py – streaming aggregates → traffic_aggregates

uvicorn dashboard_api:app --reload --port 8000 – dashboard + APIs

airflow webserver + airflow scheduler – nightly batch (peak hour)

Once all are running, you have:

Real-time streaming (Kafka + Spark + PostgreSQL)

Batch processing (Airflow nightly DAG)

API + UI (FastAPI + HTML dashboard)

Monitoring (Grafana)

9. Troubleshooting
NativeIO$Windows.access0 UnsatisfiedLinkError
→ Hadoop native libs not found.

Check C:\hadoop\bin\winutils.exe and hadoop.dll exist

Ensure HADOOP_HOME is set and on PATH

Use the extra Java options -Dhadoop.home.dir=C:\hadoop

Dashboard shows “Failed to load aggregates/raw”

Check:

Postgres is up

db_loader.py is writing into traffic_readings

traffic_streaming.py is running without errors

Kafka not receiving events

Check topic name in traffic_producer.py, db_loader.py, and Kafka config all match (e.g. traffic_raw