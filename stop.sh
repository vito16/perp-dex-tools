#!/bin/bash

# Trading Bot - Stop Script
# This script stops all running trading bots

echo "🛑 Stopping Trading Bot..."
echo ""

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
    echo "❌ Error: Docker is not running!"
    exit 1
fi

# Stop all containers
docker-compose down

if [ $? -eq 0 ]; then
    echo ""
    echo "✅ Trading bot stopped successfully"
else
    echo "❌ Failed to stop trading bot"
    exit 1
fi
