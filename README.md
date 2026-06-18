# ecommerce-streaming-pipeline
Near real-time payment monitoring pipeline — Kafka · Spark Structured Streaming · GCS · BigQuery · Grafana

> **Status:** 🚧 In progress — simulator complete, Spark job next

## Architecture
Simulador Python → Kafka → Spark Structured Streaming → GCS (Bronze) + BigQuery (Gold) → Grafana

## Stack
| Layer | Technology |
|---|---|
| Event buffer | Kafka (KRaft, Docker) |
| Processing | PySpark Structured Streaming |
| Raw storage | GCS (Parquet) |
| Warehouse | BigQuery |
| Visualization | Grafana |