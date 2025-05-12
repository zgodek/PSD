#!/bin/bash

if docker ps | grep -q "jobmanager"; then
    echo "Środowisko Kafka/Flink jest już uruchomione."
else
    echo "Uruchamiam środowisko Kafka/Flink..."
    docker compose up -d
    
    echo "Czekam na uruchomienie kontenerów..."
    sleep 10

    python3 python/initialize_data.py
fi

if [ ! -f "./flink-usrlib/flink-sql-connector-kafka-3.3.0-1.20.jar" ]; then
    echo "Brak konektora Kafka. Uruchamiam setup.sh..."
    ./scripts/t_setup.sh
fi

echo "Uruchamiam generator danych temperatury..."
python3 python/transaction_generator.py &
GENERATOR_PID=$!

# echo "Uruchamiam wizualizator transakcji..."
# python3 python/transaction_visualizer.py &
# VISUALIZER_PID=$!

echo "Uruchamiam zadanie Flink..."
./scripts/t_submit_flink_job.sh

echo "Wszystkie komponenty zostały uruchomione."
echo "Kafka UI: http://localhost:8080"
echo "Flink Dashboard: http://localhost:8081"

function cleanup {
    echo "Zatrzymywanie komponentów..."
    kill $GENERATOR_PID 2>/dev/null
    # kill $VISUALIZER_PID 2>/dev/null
    docker compose down -v
    echo "Komponenty zatrzymane."
}

trap cleanup SIGINT SIGTERM EXIT

wait