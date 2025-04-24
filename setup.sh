#!/bin/bash
echo "Setting up Python environment for Kafka-Flink integration..."
chmod +x *

pip install kafka-python matplotlib numpy apache-flink==1.17.2

mkdir -p ./flink-usrlib
chmod +777 ./flink-usrlib
mkdir -p ./flink-web-upload
chmod +777 ./flink-web-upload

echo "Downloading Kafka connector for Flink..."
wget -O ./flink-usrlib/flink-sql-connector-kafka-1.17.2.jar \
  https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/1.17.2/flink-sql-connector-kafka-1.17.2.jar

docker compose up -d
sleep 10
echo "Kopiowanie konektora Kafka do kontenera Flink..."
docker cp ./flink-usrlib/flink-sql-connector-kafka-1.17.2.jar jobmanager:/opt/flink/lib/

echo "Konfigurowanie kontenera jobmanager..."
docker exec jobmanager apt-get update
docker exec jobmanager apt-get install -y python3 python3-pip
docker exec jobmanager pip3 install numpy apache-flink==1.17.2 kafka-python
docker exec jobmanager ln -sf /usr/bin/python3 /usr/bin/python

echo "Konfigurowanie kontenera taskmanager..."
docker exec taskmanager apt-get update
docker exec taskmanager apt-get install -y python3 python3-pip
docker exec taskmanager pip3 install numpy apache-flink==1.17.2 kafka-python
docker exec taskmanager ln -sf /usr/bin/python3 /usr/bin/python
echo "Setup complete!"