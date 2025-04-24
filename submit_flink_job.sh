#!/bin/bash
PROCESSOR_FILE="temperature_processor.py"

if [ ! -f "$PROCESSOR_FILE" ]; then
    echo "Błąd: Plik $PROCESSOR_FILE nie istnieje!"
    exit 1
fi

if ! curl -s http://localhost:8081 > /dev/null; then
    echo "Błąd: Flink JobManager nie jest dostępny na http://localhost:8081"
    echo "Upewnij się, że kontenery Docker są uruchomione."
    exit 1
fi

echo "Przygotowywanie środowiska Flink..."

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

echo "Kopiowanie pliku procesora do kontenera Flink..."
docker cp "$PROCESSOR_FILE" jobmanager:/opt/flink/

echo "Uruchamianie zadania Flink..."
docker exec jobmanager flink run -py /opt/flink/temperature_processor.py

echo "Zadanie Flink zostało przesłane. Sprawdź status na http://localhost:8081"