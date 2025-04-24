#!/bin/bash

if docker ps | grep -q "jobmanager"; then
    echo "Środowisko Kafka/Flink jest już uruchomione."
else
    echo "Uruchamiam środowisko Kafka/Flink..."
    docker compose up -d
    
    echo "Czekam na uruchomienie kontenerów..."
    sleep 10
fi

if [ ! -f "./flink-usrlib/flink-sql-connector-kafka-1.17.2.jar" ]; then
    echo "Brak konektora Kafka. Uruchamiam setup.sh..."
    ./setup.sh
fi

echo "Kopiowanie konektora Kafka do kontenera Flink..."
docker cp ./flink-usrlib/flink-sql-connector-kafka-1.17.2.jar jobmanager:/opt/flink/lib/

echo "Konfiguracja środowiska Python w kontenerze Flink..."
docker exec -it jobmanager apt-get update
docker exec -it jobmanager apt-get install -y python3 python3-pip
docker exec -it jobmanager pip3 install numpy kafka-python apache-flink==1.17.2
docker exec -it jobmanager ln -sf /usr/bin/python3 /usr/bin/python

echo "Uruchamiam generator danych temperatury..."
python3 temperature_generator.py &
GENERATOR_PID=$!

echo "Uruchamiam wizualizator alarmów..."
python3 alarm_visualizer.py &
VISUALIZER_PID=$!

echo "Uruchamiam zadanie Flink..."
./submit_flink_job.sh

echo "Wszystkie komponenty zostały uruchomione."
echo "Kafka UI: http://localhost:8080"
echo "Flink Dashboard: http://localhost:8081"

function cleanup {
    echo "Zatrzymywanie komponentów..."
    kill $GENERATOR_PID 2>/dev/null
    kill $VISUALIZER_PID 2>/dev/null
    echo "Komponenty zatrzymane."
}

trap cleanup EXIT

wait