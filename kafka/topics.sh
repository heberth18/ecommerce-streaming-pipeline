#!/bin/bash
# Create all required Kafka topics for the pipeline.
# Run after every docker compose up if topics were lost (no persistent volume).

docker exec kafka /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server localhost:9092 \
    --create --topic payments \
    --partitions 3 \
    --replication-factor 1

docker exec kafka /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server localhost:9092 \
    --create --topic orders \
    --partitions 3 \
    --replication-factor 1

docker exec kafka /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server localhost:9092 \
    --create --topic payments-dlq \
    --partitions 1 \
    --replication-factor 1
