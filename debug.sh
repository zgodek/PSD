#!/bin/bash

echo "=== Docker Compose Environment Debug ==="

echo -e "\n=== Docker Containers Status ==="
docker ps -a

echo -e "\n=== Docker Networks ==="
docker network ls

echo -e "\n=== Docker Volumes ==="
docker volume ls

echo -e "\n=== Container Logs ==="
for container in zookeeper kafka kafka-ui jobmanager taskmanager; do
    echo -e "\n--- $container logs ---"
    docker logs $container 2>&1 || echo "Container $container not found or has no logs."
done

echo -e "\n=== Docker Compose Configuration ==="
cat docker-compose.yml

echo -e "\n=== System Information ==="
echo "Docker version:"
docker --version
echo "Docker Compose version:"
docker compose version
echo "System:"
uname -a