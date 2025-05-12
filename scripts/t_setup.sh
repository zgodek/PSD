#!/bin/bash
echo "Setting up Python environment for Kafka-Flink integration..."
chmod +x *

pip install redis kafka-python matplotlib numpy apache-flink==1.20

mkdir -p ./flink-usrlib
chmod +777 ./flink-usrlib
mkdir -p ./flink-web-upload
chmod +777 ./flink-web-upload

chmod +777 scripts/t_submit_flink_job.sh

echo "Downloading Kafka connector for Flink..."
wget -O ./flink-usrlib/flink-sql-connector-kafka-3.3.0-1.20.jar \
  https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.3.0-1.20/flink-sql-connector-kafka-3.3.0-1.20.jar

# docker compose up -d
# python3 python/initialize_data.py

# sleep 10
# echo "Kopiowanie konektora Kafka do kontenera Flink..."
# docker cp ./flink-usrlib/flink-sql-connector-kafka-3.3.0-1.20.jar jobmanager:/opt/flink/lib/

echo "Setup complete!"