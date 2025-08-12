#!/bin/bash

# Define the directory where your docker-compose.yml file is located
COMPOSE_DIR="./"

# Navigate to the directory containing the docker-compose.yml file
cd "$COMPOSE_DIR" || { echo "Failed to navigate to $COMPOSE_DIR"; exit 1; }

# Pull the latest images
echo "Pulling the latest Docker images..."
docker compose pull

# Build the services
echo "Building the Docker services..."
docker compose build

# Start the services
echo "Starting the Docker services..."
docker compose up -d

# Check if the services started successfully
echo "Checking the status of the Docker services..."
docker compose ps

# Wait for a few seconds to allow services to start
echo "Waiting for services to initialize..."
sleep 10

# Check the logs for any errors
echo "Checking logs for any errors..."
docker compose logs

echo "All services are running successfully!"