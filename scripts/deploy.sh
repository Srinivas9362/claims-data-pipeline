#!/bin/bash
set -e

REPO="/home/ec2-user/claims-data-pipeline"

echo "======================================"
echo "Starting Claims Data Pipeline Deploy"
echo "======================================"

cd "$REPO"

echo ">>> Fetching latest code..."
git fetch origin main

echo ">>> Resetting to origin/main..."
git reset --hard origin/main

echo ">>> Preparing Airflow log directory..."
mkdir -p "$REPO/airflow/logs"
sudo chown -R 50000:0 "$REPO/airflow/logs"
sudo chmod -R 775 "$REPO/airflow/logs"

echo ">>> Validating Docker Compose..."
docker compose config --quiet

echo ">>> Pulling latest images..."
docker compose pull

echo ">>> Starting services..."
docker compose up -d

echo ">>> Current service status..."
docker compose ps

echo "======================================"
echo "Deployment completed successfully"
echo "======================================"
