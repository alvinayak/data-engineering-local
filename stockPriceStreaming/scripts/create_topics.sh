#!/bin/bash
docker exec kafka kafka-topics \
  --create \
  --bootstrap-server localhost:9092 \
  --topic stock-prices \
  --partitions 5 \
  --replication-factor 1

echo "Topic stock-prices created with 5 partitions"