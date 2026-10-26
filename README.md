# Real-Time Financial Data Pipeline

An end-to-end data engineering project that ingests stock market data in batch and streaming modes, processes it with Apache Spark, stores it in a medallion-style data lake (Raw → Bronze → Silver), and loads it into Snowflake for analytics. The whole stack runs locally with Docker.

## Architecture

![Architecture diagram](docs/architecture.png)

**Data flow:**

1. **Sources:** Yahoo Finance and Alpha Vantage APIs provide stock price data.
2. **Ingestion:** Python producers publish records to a Kafka topic (batch pulls and a continuous stream).
3. **Processing:** Spark Structured Streaming reads from Kafka; Spark batch jobs handle cleaning and transformation.
4. **Storage:** MinIO (S3-compatible object storage) holds the data lake layers.
5. **Warehouse:** Cleaned data is loaded into Snowflake.
6. **Orchestration:** Airflow schedules and monitors the batch jobs.

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Ingestion | Python, `yfinance`, Alpha Vantage API |
| Messaging | Apache Kafka (KRaft mode) |
| Processing | Apache Spark, Spark Structured Streaming |
| Storage | MinIO (S3-compatible) |
| Orchestration | Apache Airflow |
| Data warehouse | Snowflake |
| Infrastructure | Docker, Docker Compose |
| Formats | CSV (raw), Parquet (bronze, silver) |

## Data Lake Layers

| Layer | Format | Contents |
|-------|--------|----------|
| **Raw** | CSV | Data exactly as received from the stream; never modified |
| **Bronze** | Parquet | Real-time data in columnar format with a defined schema |
| **Silver** | Parquet | Deduplicated, validated, and type-checked data ready for analytics |

## Project Structure

```
realtime-pipeline/
├── docker-compose.yml
├── producer.py              # Fetches stock data and publishes to Kafka
├── consumer.py              # Simple Kafka consumer used for testing
├── spark_to_raw.py          # Spark streaming job: Kafka -> Raw layer
├── raw_to_silver.py         # Spark batch job: Raw -> Silver layer
├── dags/
│   └── pipeline_dag.py      # Airflow DAG
├── docs/
│   └── architecture.png
└── README.md
```

## Prerequisites

- Docker Desktop (with Docker Compose)
- Python 3.9+
- A free [Alpha Vantage API key](https://www.alphavantage.co/support/#api-key)
- A Snowflake account (free trial works)

## Getting Started

### 1. Clone the repository

```bash
git clone https://github.com/Madan8280/realtime-pipeline.git
cd realtime-pipeline
```

### 2. Start the infrastructure

```bash
docker compose up -d
docker ps    # confirm all containers are running
```

### 3. Create the Kafka topic

```bash
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --create \
  --topic stock-prices \
  --bootstrap-server localhost:9092 \
  --partitions 1 --replication-factor 1
```

### 4. Create the MinIO buckets

Open the MinIO console at http://localhost:9001 and create three buckets: `raw`, `bronze`, `silver`.

### 5. Install Python dependencies

```bash
pip install kafka-python yfinance boto3 snowflake-connector-python
```

### 6. Run the pipeline

```bash
python producer.py          # start publishing stock data
```

In a second terminal, submit the Spark streaming job to the Spark master container (see `spark_to_raw.py`). Then enable the `stock_pipeline` DAG in the Airflow UI to schedule the Raw → Silver job.

## Service URLs

| Service | URL |
|---------|-----|
| MinIO console | http://localhost:9001 |
| Spark master UI | http://localhost:8080 |
| Airflow UI | http://localhost:8081 |

## Configuration

Credentials in `docker-compose.yml` (such as the default MinIO user and password) are for local development only. Do not commit real Snowflake or API credentials; keep them in a `.env` file listed in `.gitignore`.

## Project Status

- [ ] Kafka running in Docker, tested with console producer/consumer
- [ ] Python producer publishing live stock data
- [ ] MinIO buckets created and accessible
- [ ] Spark streaming job writing to Raw layer
- [ ] Raw → Silver transformation job
- [ ] Airflow DAG scheduling the batch job
- [ ] Silver data loaded into Snowflake

> Tick these off as you complete them, and remove anything you did not build before publishing.

## Results

[FILL IN: e.g. records processed per run, end-to-end latency, number of symbols tracked, a screenshot of the Snowflake table or a dashboard.]

## Limitations and Future Work

- Alpha Vantage's free tier is rate-limited, which caps how "real-time" the stream can be.
- Add a Gold layer with business-level aggregates (daily averages, moving averages).
- Add data quality checks (for example with Great Expectations) between layers.
- Add monitoring and alerting for failed jobs.
- Load directly from MinIO to Snowflake using an external stage.

## Author

**Venkata Madan Vipparla**
[LinkedIn](https://www.linkedin.com/in/madan-vipparla) · [GitHub](https://github.com/Madan8280)
