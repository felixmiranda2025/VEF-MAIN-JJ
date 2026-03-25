@echo off
title VEF ERP - Servidor
color 1F
cls

echo.
echo  ==========================================
echo   VEF AUTOMATIZACION - ERP Industrial
echo  ==========================================
echo.

:: Ir a la carpeta del .bat (siempre)
cd /d "%~dp0"
echo  Carpeta: %CD%
echo.

:: ══════════════════════════════════════════
:: 1. NODE.JS
:: ══════════════════════════════════════════
echo  [1/6] Verificando Node.js...
node -v >nul 2>&1
if %errorlevel% neq 0 (
    echo.
    echo  XCXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
    echo  X  ERROR: Node.js no esta instalado       X
    echo  X  Descarga: https://nodejs.org           X
    echo  X  Instala la version LTS                 X
    echo  XCXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
    echo.
    pause
    exit /b 1
)
for /f %%v in ('node -v') do echo  [OK] Node.js %%v

:: ══════════════════════════════════════════
:: 2. NODE_MODULES
:: ══════════════════════════════════════════
echo.
echo  [2/6] Verificando dependencias Node...
if not exist "node_modules\express" (
    echo  Instalando... puede tardar 1-2 min la primera vez
    npm install
    if %errorlevel% neq 0 (
        echo  [ERROR] npm install fallo
        pause
        exit /b 1
    )
    echo  [OK] Dependencias instaladas
) else (
    echo  [OK] node_modules existe
)

:: ══════════════════════════════════════════
:: 3. PYTHON
:: ══════════════════════════════════════════
echo.
echo  [3/6] Buscando Python...
set PYTHON_CMD=

python -V >nul 2>&1
if %errorlevel% equ 0 ( set PYTHON_CMD=python & goto python_ok )

py -V >nul 2>&1
if %errorlevel% equ 0 ( set PYTHON_CMD=py & goto python_ok )

python3 -V >nul 2>&1
if %errorlevel% equ 0 ( set PYTHON_CMD=python3 & goto python_ok )

echo  [INFO] Python no encontrado - modulo SAT desactivado
goto skip_sat

:python_ok
for /f "tokens=*" %%v in ('%PYTHON_CMD% -V 2^>^&1') do echo  [OK] %%v

:: ══════════════════════════════════════════
:: 4. DEPENDENCIAS PYTHON
:: ══════════════════════════════════════════
echo.
echo  [4/6] Verificando dependencias Python...
if not exist "sat_service\sat_api.py" (
    echo  [INFO] sat_service\sat_api.py no encontrado
    goto skip_sat
)

%PYTHON_CMD% -c "import flask" >nul 2>&1
if %errorlevel% neq 0 (
    echo  Instalando flask requests lxml pyOpenSSL chilkat2...
    %PYTHON_CMD% -m pip install flask requests lxml pyOpenSSL chilkat2 -q
    if %errorlevel% neq 0 (
        echo  [AVISO] pip install fallo - SAT puede no funcionar
        goto skip_sat
    )
    echo  [OK] Dependencias Python instaladas
) else (
    echo  [OK] Dependencias Python listas
)

:: ══════════════════════════════════════════
:: 5. INICIAR SAT
:: ══════════════════════════════════════════
echo.
echo  [5/6] Iniciando servicio SAT puerto 5050...

powershell -Command "try{(Invoke-WebRequest http://localhost:5050/health -TimeoutSec 1).StatusCode}catch{'off'}" 2>nul | findstr "200" >nul 2>&1
if %errorlevel% equ 0 (
    echo  [OK] SAT ya estaba corriendo
    goto skip_sat
)

start "VEF SAT Service" /min %PYTHON_CMD% sat_service\sat_api.py
echo  Esperando SAT max 10 seg...

set WAIT=0
:wait_loop
timeout /t 1 /nobreak >nul
set /a WAIT+=1
powershell -Command "try{(Invoke-WebRequest http://localhost:5050/health -TimeoutSec 1).StatusCode}catch{'off'}" 2>nul | findstr "200" >nul 2>&1
if %errorlevel% equ 0 ( echo  [OK] SAT listo en %WAIT% seg & goto skip_sat )
if %WAIT% lss 10 goto wait_loop
echo  [INFO] SAT sigue iniciando en segundo plano...

:skip_sat

:: ══════════════════════════════════════════
:: 6. PUERTO 3000
:: ══════════════════════════════════════════
echo.
echo  [6/6] Verificando puerto 3000...
netstat -ano 2>nul | findstr ":3000 " | findstr "LISTEN" >nul 2>&1
if %errorlevel% equ 0 (
    echo  [INFO] Puerto 3000 ocupado - abriendo navegador
    start "" http://localhost:3000
    echo.
    echo  Servidor ya estaba corriendo.
    echo  Si la pagina no carga cierra esta ventana y vuelve a ejecutar.
    echo.
    pause
    exit /b 0
)
echo  [OK] Puerto 3000 libre

:: ══════════════════════════════════════════
:: INICIAR VEF ERP
:: ══════════════════════════════════════════
echo.
echo  ==========================================
echo   LISTO - Iniciando VEF ERP
echo   URL: http://localhost:3000
echo   NO cierres esta ventana
echo   Para salir presiona Ctrl+C
echo  ==========================================
echo.

start /b cmd /c "timeout /t 3 /nobreak >nul && start http://localhost:3000"

node server.js

echo.
echo  El servidor se detuvo. Revisa el error arriba.
pause
