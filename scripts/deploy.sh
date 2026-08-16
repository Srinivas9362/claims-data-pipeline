#!/bin/bash

set -e

echo "======================================"
echo "Starting Claims Data Pipeline Deploy"
echo "======================================"

cd ~/claims-data-pipeline

echo ">>> Fetching latest code..."
git fetch origin main

echo ">>> Resetting to origin/main..."
git reset --hard origin/main

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