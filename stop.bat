@echo off
REM Trading Bot - Stop Script for Windows
REM This script stops all running trading bots

echo 🛑 Stopping Trading Bot...
echo.

REM Check if Docker is running
docker info >nul 2>&1
if errorlevel 1 (
    echo ❌ Error: Docker is not running!
    pause
    exit /b 1
)

REM Stop all containers
docker-compose down

if errorlevel 0 (
    echo.
    echo ✅ Trading bot stopped successfully
    pause
) else (
    echo ❌ Failed to stop trading bot
    pause
    exit /b 1
)
