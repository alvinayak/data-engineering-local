

# Real-time stock price streaming pipeline
## Project summary — Kafka + PyFlink + PostgreSQL

---

## Overview

A local end-to-end real-time data engineering pipeline built over a weekend as part of SDE3 interview preparation. The pipeline simulates live stock price events, streams them through Kafka, processes them with PyFlink using 1-minute OHLC windowed aggregations, detects price anomalies, and stores results in PostgreSQL. A Jupyter dashboard visualizes the data in real time.

---

## Goals

- Build a production-grade streaming pipeline locally using Docker
- Learn Kafka partitioning, watermarks, windowing, and checkpointing hands-on
- Implement idempotent writes using PostgreSQL upsert semantics
- Use Snowflake IDs for unique event identification and audit
- Practice concepts directly relevant to fintech SDE3 interviews (JPMorgan, Swiss Re, Airbus)

---

## Tech stack

| Component | Technology | Purpose |
|-----------|-----------|---------|
| Message broker | Apache Kafka (Confluent 7.4.0) | Stream stock price events |
| Coordination | Apache Zookeeper | Kafka cluster coordination |
| Stream processing | Apache Flink 1.17 (PyFlink) | Windowed OHLC aggregation |
| Storage | PostgreSQL with pgvector | Store OHLC results and anomalies |
| Object storage | MinIO | Local S3-equivalent |
| Notebook | Jupyter (PySpark notebook) | Dashboard and visualization |
| Language | Python 3.10 (pyenv + venv) | Producer, Flink job, dashboard |
| Infrastructure | Docker Desktop (Mac Apple Silicon) | Local container orchestration |

---

## Architecture
Python producer (Mac)
↓ key = stock symbol
Kafka topic: stock-prices (5 partitions)
↓ one partition per stock
PyFlink job (inside Docker)
├── Source operator: reads from kafka:29092
├── KeyBy operator: hash(symbol) % 5
├── Window operator: 1-min tumbling, event time, 10s grace period
│   ├── OHLC aggregation per window
│   └── Anomaly detection (price move >= 2%)
└── Sink: writes to PostgreSQL
PostgreSQL (pgvector container)
├── ohlc_results table
└── anomalies table
Jupyter dashboard
└── Live charts refreshing every 30s

---

## Key design decisions

### Partitioning strategy
Each stock symbol (AAPL, GOOGL, MSFT, AMZN, JPM) maps to one Kafka partition using hash(symbol) % 5. This ensures all events for a given stock land on the same partition, making windowed aggregation correct and efficient without cross-partition coordination.

### Snowflake IDs
Custom 64-bit Snowflake ID generator. Encodes timestamp (41 bits) + datacenter ID (5 bits) + machine ID (5 bits) + sequence (12 bits). Gives time-ordered, sortable IDs with embedded creation timestamp and cheaper PostgreSQL indexing vs UUID.

### Watermarks and late data
Flink uses event time processing with 10-second bounded out-of-orderness watermark. Grace period = 10000ms. Late events update OHLC results, PostgreSQL ON CONFLICT DO UPDATE handles re-emissions idempotently.

### Idempotency
ON CONFLICT (symbol, window_start) with WHERE last_message_id != EXCLUDED.last_message_id prevents duplicate rows on Flink restarts.

### Checkpointing
Flink checkpoints every 60 seconds. On restart, window state restores from checkpoint and Kafka offsets resume — exactly-once semantics.

---

## Event schema

{
  "message_id": 311852626922835968,
  "symbol": "AAPL",
  "price": 151.23,
  "event_time": "2026-05-10T13:11:45.123456+00:00",
  "volume": 4521
}

---

## OHLC window result example

symbol:       AAPL
window_start: 2026-05-10 13:11:00
window_end:   2026-05-10 13:12:00
open:         190.45
high:         205.18
low:          186.79
close:        202.19
count:        59

---

## PostgreSQL schema

CREATE TABLE ohlc_results (
    symbol              VARCHAR(10)   NOT NULL,
    window_start        TIMESTAMP     NOT NULL,
    window_end          TIMESTAMP     NOT NULL,
    open                DECIMAL(12,2),
    high                DECIMAL(12,2),
    low                 DECIMAL(12,2),
    close               DECIMAL(12,2),
    event_count         INT,
    first_message_id    BIGINT,
    last_message_id     BIGINT,
    created_at          TIMESTAMP     DEFAULT NOW(),
    PRIMARY KEY (symbol, window_start)
);

CREATE TABLE anomalies (
    id                  BIGINT        PRIMARY KEY,
    symbol              VARCHAR(10)   NOT NULL,
    window_start        TIMESTAMP     NOT NULL,
    price_change_pct    DECIMAL(8,4),
    open                DECIMAL(12,2),
    close               DECIMAL(12,2),
    detected_at         TIMESTAMP     DEFAULT NOW()
);

---

## Infrastructure issues solved

| Problem | Root cause | Fix |
|---------|-----------|-----|
| Disk full 117MB | AvzMemory APFS partition | Deleted partition, freed 96GB |
| PyFlink incompatible Python 3.12 | apache-beam requires older Python | pyenv Python 3.10.14 + venv |
| Beam coder type mismatch | Beam 2.43.0 vs PyFlink 1.17 | Ran job inside Flink Docker container |
| python not found in container | Container has python3 only | ln -s /usr/bin/python3 /usr/bin/python |
| PyFlink install fails in container | JRE not JDK, no include headers | Installed openjdk-11-jdk inside container |
| Kafka not reachable from container | Bootstrap server was localhost:9092 | Changed to kafka:29092 |
| allowed_lateness type error | PyFlink 1.17 expects int ms not Time | Changed to 10000 |
| Cloudpickle fails on window function | Snowflake generator has thread lock | Moved instantiation inside write_ohlc |
| ohlc_results table not found | Schema not applied | docker exec -i pgvector psql < sql/schema.sql |

---

## Concepts learned

- Kafka partitioning — key-based routing, deterministic partition assignment
- Watermarks — per-partition progress tracking, event time vs processing time
- Tumbling windows — fixed non-overlapping time buckets, OHLC aggregation
- Flink operator hierarchy — source, keyBy, window, sink, parallelism
- Checkpointing — barrier injection, RocksDB state snapshots, exactly-once
- Idempotency — ON CONFLICT DO UPDATE, message_id guard
- Snowflake IDs — timestamp encoding, sortability, embedded metadata
- Docker networking — container DNS, localhost vs container name, port mapping

---

## Next steps

- Connect Jupyter dashboard via host.docker.internal:5432
- Submit Flink job via flink run instead of python3 directly
- Add idempotency store and audit reconciliation job
- Extend with real financial data (Yahoo Finance, Alpha Vantage)
- Add Kafka consumer group lag monitoring
- Deploy to AWS (MSK, EMR, RDS)

---

## How to run

docker-compose up -d
docker exec -i pgvector psql -U admin -d vectordb < sql/schema.sql
python scripts/producer.py

docker cp scripts/flink_job.py flink-jobmanager:/opt/flink/usrlib/flink_job.py
docker exec -it flink-jobmanager bash -c "
  export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-arm64 &&
  export PYTHONPATH=/opt/flink/usrlib &&
  python3 /opt/flink/usrlib/flink_job.py
"

docker exec -it pgvector psql -U admin -d vectordb \
  -c "SELECT symbol, window_start, open, high, low, close FROM ohlc_results ORDER BY window_start DESC LIMIT 10;"

---
