#!/bin/bash
set -e

REPO="/home/ec2-user/claims-data-pipeline"

echo "======================================"
echo "Starting Claims Data Pipeline Deploy"
echo "======================================"

cd "$REPO"

echo ">>> Fetching latest code..."
sudo -u ec2-user git fetch origin main

echo ">>> Resetting to origin/main..."
sudo -u ec2-user git reset --hard origin/main

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
