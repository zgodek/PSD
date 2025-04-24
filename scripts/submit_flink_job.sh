#!/bin/bash
PROCESSOR_FILE="python/temperature_processor.py"

if [ ! -f "$PROCESSOR_FILE" ]; then
    echo "Błąd: Plik $PROCESSOR_FILE nie istnieje!"
    exit 1
fi

if ! curl -s http://localhost:8081 > /dev/null; then
    echo "Błąd: Flink JobManager nie jest dostępny na http://localhost:8081"
    echo "Upewnij się, że kontenery Docker są uruchomione."
    exit 1
fi

echo "Kopiowanie pliku procesora do kontenera Flink..."
docker cp "$PROCESSOR_FILE" jobmanager:/opt/flink/

echo "Uruchamianie zadania Flink..."
docker exec jobmanager flink run -py /opt/flink/temperature_processor.py

echo "Zadanie Flink zostało przesłane. Sprawdź status na http://localhost:8081"