@echo off
REM Trading Bot - Quick Start Script for Windows
REM This script makes it easy to run the trading bot with Docker

echo 🤖 Starting Trading Bot with Docker...
echo.

REM Check if Docker is running
docker info >nul 2>&1
if errorlevel 1 (
    echo ❌ Error: Docker is not running!
    echo    Please start Docker Desktop and try again.
    pause
    exit /b 1
)

REM Check if .env file exists
if not exist .env (
    echo ⚠️  Warning: .env file not found!
    if exist .env.example (
        echo    Copying .env.example to .env...
        copy .env.example .env
        echo ✅ Created .env file. Please edit it with your API keys before running again.
        pause
        exit /b 1
    ) else (
        echo ❌ Error: .env.example not found. Cannot create .env file.
        pause
        exit /b 1
    )
)

echo ✅ Docker is running
echo ✅ .env file found
echo.

REM Build and start the bot
echo 📦 Building Docker image...
docker-compose build

if errorlevel 0 (
    echo.
    echo 🚀 Starting trading bot...
    echo    Press Ctrl+C to stop the bot
    echo.
    docker-compose up
) else (
    echo ❌ Failed to build Docker image
    pause
    exit /b 1
)
