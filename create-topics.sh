#!/bin/bash
echo "Waiting for Kafka to be ready..."
until docker exec kafka kafka-topics --bootstrap-server kafka:9092 --list > /dev/null 2>&1; do
  echo "Kafka not ready yet, waiting..."
  sleep 5
done

echo "Creating Kafka topics..."

docker exec kafka kafka-topics --create --topic Temperatura --bootstrap-server kafka:9092 --partitions 3 --replication-factor 1 --if-not-exists

docker exec kafka kafka-topics --create --topic Alarm --bootstrap-server kafka:9092 --partitions 3 --replication-factor 1 --if-not-exists

echo "Listing all topics:"
docker exec kafka kafka-topics --bootstrap-server kafka:9092 --list

echo "Kafka topics created successfully!"