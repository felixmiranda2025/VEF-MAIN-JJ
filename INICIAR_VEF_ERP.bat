@echo off
title VEF ERP - Servidor
color 1F
cls
echo.
echo  ==========================================
echo   VEF AUTOMATIZACION - ERP Industrial
echo   http://localhost:3000
echo  ==========================================
echo.

cd /d "%~dp0"

where node >nul 2>&1
if %errorlevel% neq 0 (
    echo  [ERROR] Node.js no esta instalado.
    echo  Descargalo en: https://nodejs.org
    echo  Instala la version LTS y vuelve a ejecutar este archivo.
    pause
    exit /b 1
)

if not exist "node_modules" (
    echo  Primera vez: instalando dependencias...
    echo  Espera un momento...
    npm install
    echo.
)

echo  Iniciando servidor VEF ERP...
start "" cmd /c "timeout /t 3 /nobreak >nul && start http://localhost:3000"
echo  Navegador abriendo en 3 segundos...
echo.
echo  IMPORTANTE: La URL debe ser http://localhost:3000
echo  Para cerrar el servidor presiona Ctrl+C
echo.

node server.js
pause
