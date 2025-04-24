#!/bin/bash

# Ścieżka do pliku procesora
PROCESSOR_FILE="temperature_processor.py"

# Sprawdź, czy plik istnieje
if [ ! -f "$PROCESSOR_FILE" ]; then
    echo "Błąd: Plik $PROCESSOR_FILE nie istnieje!"
    exit 1
fi

# Sprawdź, czy Flink jest uruchomiony
if ! curl -s http://localhost:8081 > /dev/null; then
    echo "Błąd: Flink JobManager nie jest dostępny na http://localhost:8081"
    echo "Upewnij się, że kontenery Docker są uruchomione."
    exit 1
fi

echo "Przygotowywanie środowiska Flink..."

# Instaluj Python i wymagane pakiety w kontenerze jobmanager
echo "Konfigurowanie kontenera jobmanager..."
docker exec jobmanager apt-get update
docker exec jobmanager apt-get install -y python3 python3-pip
docker exec jobmanager pip3 install numpy apache-flink==1.17.2 kafka-python

# Utworzenie linku symbolicznego python -> python3 w jobmanager
docker exec jobmanager ln -sf /usr/bin/python3 /usr/bin/python

# Instaluj Python i wymagane pakiety w kontenerze taskmanager
echo "Konfigurowanie kontenera taskmanager..."
docker exec taskmanager apt-get update
docker exec taskmanager apt-get install -y python3 python3-pip
docker exec taskmanager pip3 install numpy apache-flink==1.17.2 kafka-python

# Utworzenie linku symbolicznego python -> python3 w taskmanager
docker exec taskmanager ln -sf /usr/bin/python3 /usr/bin/python

echo "Creating Kafka topics..."
# Tutaj możesz dodać kod do tworzenia tematów Kafka, jeśli jest potrzebny

# Kopiuj plik procesora do kontenera
echo "Kopiowanie pliku procesora do kontenera Flink..."
docker cp "$PROCESSOR_FILE" jobmanager:/opt/flink/

# Uruchom zadanie Flink
echo "Uruchamianie zadania Flink..."
docker exec jobmanager flink run -py /opt/flink/temperature_processor.py

echo "Zadanie Flink zostało przesłane. Sprawdź status na http://localhost:8081"