## Soccer Real-Time Data Engineering Platform
An end-to-end real-time data engineering platform that ingests soccer match events, processes streaming data using Apache Kafka and Spark Structured Streaming, and builds analytics-ready datasets using a Delta Lake architecture.

This project simulates a modern streaming data platform used in production environments for event-driven analytics and real-time sports data processing.

## Project Objectives

- Build a real-time event streaming pipeline
- Implement a Medallion Data Lake Architecture (Bronze → Silver → Gold)
- Process streaming events using Spark Structured Streaming
- Persist scalable data lake storage using Delta Lake on AWS S3
- Load analytics-ready datasets into PostgreSQL
- Implement CI validation using GitHub Actions
- Demonstrate production-grade data engineering architecture

## Technology Stack

| Layer | Technology |
|------|-------------|
| Event Generation | Python |
| Streaming Platform | Apache Kafka |
| Stream Processing | Apache Spark Structured Streaming |
| Data Lake Storage | Delta Lake |
| Cloud Storage | AWS S3 |
| Analytics Database | PostgreSQL |
| Infrastructure | Docker Compose |
| CI/CD | GitHub Actions |
| Version Control | GitHub |

## High-Level Architecture

```
Kaggle Soccer Dataset (SQLite)
            │
            ▼
Python Event Generator
(Kafka Producer)
            │
            ▼
Kafka Topic — soccer_events
(Docker Kafka Cluster)
            │
            ▼
Spark Structured Streaming
            │
            ▼
Delta Lake Data Lake (AWS S3)
     ┌───────────┬───────────┬───────────┐
     │  Bronze   │  Silver   │   Gold    │
     │ Raw Data  │ Cleaned   │ Analytics │
     └───────────┴───────────┴───────────┘
            │
            ▼
PostgreSQL Analytics Database
```

## Pipeline Components

# Phase 1 — Event Streaming (Kafka Producer)

Component: event_producer.py

This phase simulates real-time soccer events using the Kaggle European Soccer database.

Responsibilities:

- Connects to SQLite soccer dataset
- Randomly generates match events
- Publishes events to Kafka topic soccer_events
- Simulates event streaming at controlled intervals

Example Event Structure

```
{
  event_id
  match_id
  league
  season
  minute
  event_type
  player
  team
  xg
  is_home
  event_time
  ingestion_time
}
```

# Phase 2 — Bronze Streaming Layer (Raw Data)

Spark Job: local_spark_stream.py

The Bronze layer ingests raw streaming events directly from Kafka and stores them in Delta Lake without transformations.

Task Flow

```
Kafka Topic
     ↓
Spark Structured Streaming
     ↓
Bronze Delta Table (S3)
```

Responsibilities

- Consumes Kafka topic in real time
- Parses JSON event schema
- Writes raw events into Delta Lake
- Maintains checkpointing for fault tolerance

Purpose in Architecture

- Preserve immutable raw event history
- Enable replay of events
- Provide audit trail for downstream processing

# Phase 3 — Silver Streaming Layer (Data Cleaning)

Spark Job: silver_stream.py

The Silver layer performs data cleansing and transformation on Bronze data.

Task Flow

```
Bronze Delta Stream
      ↓
Data Cleaning
      ↓
Silver Delta Table
```

Responsibilities

- Cast timestamp fields
- Remove malformed records
- Normalize player names
- Validate event schema consistency

Example Transformation

player = trim(split(player, ",")[0])

Purpose

- Standardize data quality
- Prepare datasets for analytics aggregation

# Phase 4 — Gold Streaming Layer (Analytics Aggregations)

Spark Jobs

- gold_match_stats_stream.py
- gold_team_metrics_stream.py
- gold_player_metrics_stream.py

The Gold layer builds streaming aggregations that power analytics use cases.

Task Flow

```
Silver Stream
     ↓
Streaming Aggregations
     ↓
Gold Analytics Tables
```

Generated Tables

- match_stats: 
- team_metrics
- player_metrics

Purpose

- Provide real-time analytics
- Enable downstream BI queries
- Aggregate event streams into usable metrics

# Phase 5 — Data Lake Storage (AWS S3)

All Delta tables are stored in an AWS S3 data lake.

Data Lake Layout

s3://soccer-realtime-pipeline-lake/

```
bronze/
silver/
gold/
   ├── match_stats
   ├── team_metrics
   └── player_metrics
```

Responsibilities

- Persist streaming results
- Enable scalable cloud storage
- Maintain Delta transaction logs

# Phase 6 — Analytics Database Load

Script: load_gold_to_postgres.py

Gold analytics tables are loaded into PostgreSQL for query performance.

Task Flow

```
Delta Gold Tables
      ↓
Spark JDBC
      ↓
PostgreSQL Analytics Tables
```

Responsibilities

- Load aggregated metrics
- Create analytics tables
- Enable SQL-based analysis

Example Query

```
SELECT player, goals
FROM player_metrics
ORDER BY goals DESC
LIMIT 10;
```

# Phase 7 — CI/CD Validation

GitHub Actions automatically validates the repository on every push.

Workflow: .github/workflows/pipeline.yml

Validation Steps

```
Repository Checkout
        ↓
Python Setup
        ↓
Dependency Installation
        ↓
Code Linting (flake8)
        ↓
Python Script Validation
        ↓
Project Structure Checks
```

Purpose

- Prevent broken commits
- Maintain code quality
- Ensure pipeline integrity

## Medallion Data Lake Architecture
```
        Kafka Events
             │
             ▼
        Bronze Layer
   (Raw Streaming Data)
             │
             ▼
        Silver Layer
   (Cleaned / Validated)
             │
             ▼
        Gold Layer
   (Analytics Aggregations)
             │
             ▼
        PostgreSQL
        Analytics DB
```

## Engineering Challenges Solved

- Streaming integration between Kafka and Spark
- Delta Lake checkpoint management
- Kafka broker connectivity across WSL and Docker
- Player name normalization issues from raw datasets
- Git repository size management for streaming artifacts
- S3 integration using Hadoop s3a connector
- Spark dependency conflicts during streaming jobs

## Results

- Fully functional real-time streaming data platform
- End-to-end Kafka → Spark → Data Lake architecture
- Cloud-integrated Delta Lake storage
- Analytics-ready PostgreSQL tables
- Automated CI validation pipeline
- Production-style data engineering project

## Author

Andres Lacayo  
Data Engineering Portfolio Project