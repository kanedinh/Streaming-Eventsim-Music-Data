@echo off

set NETWORK_NAME=shared_network

:: 1. Check if the Docker network already exists
docker network ls | findstr /C:"%NETWORK_NAME%" >nul
if %errorlevel% equ 0 (
    echo  Network '%NETWORK_NAME%' already exists. Skipping network creation.
) else (
    echo  Creating Docker network '%NETWORK_NAME%'...
    docker network create %NETWORK_NAME%
    echo  Network '%NETWORK_NAME%' created successfully.
)

:: 2. Start all containers in detached mode
echo  Starting kafka containers in detached mode...
cd ..\kafka
docker-compose up -d

:: 3. Wait for a few seconds to ensure all containers are up
echo  Done!
:: pause