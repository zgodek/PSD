#!/bin/bash
echo "Setting up Python environment for Kafka-Flink integration..."

# Instalacja wymaganych pakietów Python
pip install kafka-python matplotlib numpy apache-flink==1.17.2

# Tworzenie katalogu dla JAR-ów Flinka
mkdir -p ./flink-usrlib
mkdir -p ./flink-web-upload

# Pobieranie konektora Kafka dla Flinka
echo "Downloading Kafka connector for Flink..."
wget -O ./flink-usrlib/flink-sql-connector-kafka-1.17.2.jar \
  https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/1.17.2/flink-sql-connector-kafka-1.17.2.jar

# Utworzenie linku symbolicznego python -> python3 w systemie lokalnym
if [ ! -f /usr/bin/python ]; then
  echo "Creating symbolic link for python -> python3 in local system..."
  sudo ln -sf /usr/bin/python3 /usr/bin/python
fi

echo "Setup complete!"