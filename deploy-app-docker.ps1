# deploy-app-docker.ps1
# Ejecutar con: .\deploy-app-docker.ps1
$ErrorActionPreference = 'Stop'

$NETWORK_NAME = 'bio-network'
$SCRIPT_DIR = Split-Path -Parent $MyInvocation.MyCommand.Definition

$STACK_SERVICIO_DIR = Join-Path $SCRIPT_DIR 'docker-servicios-bio\docker-servicios-bio'
$STACK_WEB_DIR = Join-Path $SCRIPT_DIR 'tfg\docker'

function Ensure-Network {
    param($name)
    try {
        docker network inspect $name > $null 2>&1
        Write-Host "La red $name ya existe"
    } catch {
        Write-Host "Creando red Docker: $name"
        docker network create $name
    }
}

Ensure-Network $NETWORK_NAME

Write-Host "Desplegando Servicio Conversión..."
if (Get-Command docker-compose -ErrorAction SilentlyContinue) {
    docker-compose -f (Join-Path $STACK_SERVICIO_DIR 'docker-compose.yml') -p bio-service up -d --build --no-cache
} else {
    docker compose -f (Join-Path $STACK_SERVICIO_DIR 'docker-compose.yml') -p bio-service up -d --build --no-cache
}

Write-Host "Desplegando Servicio Web..."
if (Get-Command docker-compose -ErrorAction SilentlyContinue) {
    docker-compose -f (Join-Path $STACK_WEB_DIR 'docker-compose.yml') -p bio-web up -d --build
} else {
    docker compose -f (Join-Path $STACK_WEB_DIR 'docker-compose.yml') -p bio-web up -d --build
}

Write-Host "Stacks desplegados correctamente en la red $NETWORK_NAME"