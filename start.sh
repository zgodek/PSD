#!/bin/bash
check_containers() {
    echo "Checking if all required containers are running..."
    
    REQUIRED_CONTAINERS=("zookeeper" "kafka" "kafka-ui" "jobmanager" "taskmanager")
    MISSING_CONTAINERS=()
    
    for container in "${REQUIRED_CONTAINERS[@]}"; do
        if ! docker ps --format '{{.Names}}' | grep -q "^${container}$"; then
            MISSING_CONTAINERS+=("$container")
        fi
    done
    
    if [ ${#MISSING_CONTAINERS[@]} -gt 0 ]; then
        echo "WARNING: The following containers are not running:"
        for container in "${MISSING_CONTAINERS[@]}"; do
            echo "  - $container"
        done
        
        echo "Checking container logs for errors..."
        for container in "${MISSING_CONTAINERS[@]}"; do
            echo "=== Logs for $container (if it was created) ==="
            docker logs "$container" 2>&1 || echo "Container $container was not created or has no logs."
            echo "=============================================="
        done
        
        return 1
    else
        echo "All required containers are running!"
        return 0
    fi
}

restart_environment() {
    echo "Stopping the environment..."
    docker compose down
    
    echo "Removing any leftover containers..."
    docker rm -f zookeeper kafka kafka-ui jobmanager taskmanager 2>/dev/null || true
    
    echo "Preparing Flink directories..."
    mkdir -p ./flink-usrlib/flink-web-upload
    chmod -R 777 ./flink-usrlib
    
    echo "Starting the environment again..."
    docker compose up -d
    
    echo "Waiting for services to be ready..."
    sleep 15
}

chmod +x *.sh

echo "Preparing Flink directories..."
mkdir -p ./flink-usrlib/flink-web-upload
chmod -R 777 ./flink-usrlib

echo "Starting Kafka and Flink environment..."
docker compose up -d

echo "Waiting for services to be ready..."
sleep 10

if ! check_containers; then
    echo "Some containers failed to start. Attempting to restart the environment..."
    restart_environment
    
    if ! check_containers; then
        echo "ERROR: Environment setup failed even after restart."
        echo "Please check your docker-compose.yml configuration."
        exit 1
    fi
fi

./create-topics.sh

echo "Environment is ready!"
echo "Kafka UI: http://localhost:8080"
echo "Flink Dashboard: http://localhost:8081"