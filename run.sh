#!/bin/bash

# Trading Bot - Quick Start Script
# This script makes it easy to run the trading bot with Docker

echo "🤖 Starting Trading Bot with Docker..."
echo ""

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
    echo "❌ Error: Docker is not running!"
    echo "   Please start Docker Desktop and try again."
    exit 1
fi

# Check if .env file exists
if [ ! -f .env ]; then
    echo "⚠️  Warning: .env file not found!"
    echo "   Copying .env.example to .env..."
    if [ -f .env.example ]; then
        cp .env.example .env
        echo "✅ Created .env file. Please edit it with your API keys before running again."
        exit 1
    else
        echo "❌ Error: .env.example not found. Cannot create .env file."
        exit 1
    fi
fi

echo "✅ Docker is running"
echo "✅ .env file found"
echo ""

# Build and start the bot
echo "📦 Building Docker image..."
docker-compose build

if [ $? -eq 0 ]; then
    echo ""
    echo "🚀 Starting trading bot..."
    echo "   Press Ctrl+C to stop the bot"
    echo ""
    docker-compose up
else
    echo "❌ Failed to build Docker image"
    exit 1
fi
