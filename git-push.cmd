@echo off
chcp 65001 >nul
title VEF ERP — Subir a Git

cd /d D:\VEF\VEF15\vef-erp

echo.
echo ============================================
echo   VEF ERP — Subir cambios a GitHub
echo ============================================
echo.

:: Verificar que estamos en un repo git
if not exist ".git" (
    echo [ERROR] No se encontro repositorio Git en esta carpeta.
    echo Ejecuta primero:  git init
    pause
    exit /b 1
)

:: Mostrar estado actual
echo [1/4] Estado actual del repositorio:
echo --------------------------------------------
git status
echo.

:: Preguntar mensaje del commit
set /p MENSAJE="[2/4] Mensaje del commit (Enter = 'Actualizacion VEF ERP'): "
if "%MENSAJE%"=="" set MENSAJE=Actualizacion VEF ERP

:: Agregar todos los cambios
echo.
echo [3/4] Agregando todos los cambios...
git add .
if errorlevel 1 (
    echo [ERROR] Fallo git add
    pause
    exit /b 1
)
echo     OK - Archivos preparados.

:: Hacer commit
echo.
git commit -m "%MENSAJE%"
if errorlevel 1 (
    echo [AVISO] Sin cambios nuevos para commitear, o error en commit.
    pause
    exit /b 1
)

:: Subir al repositorio remoto
echo.
echo [4/4] Subiendo a GitHub...
git push
if errorlevel 1 (
    echo.
    echo [AVISO] Fallo git push. Posibles causas:
    echo   - No tienes remote configurado (usa: git remote add origin URL)
    echo   - No estas autenticado con GitHub
    echo   - La rama no existe en remoto (usa: git push -u origin main)
    echo.
    echo Intentando con: git push -u origin main ...
    git push -u origin main
)

echo.
echo ============================================
echo   Listo! Cambios subidos correctamente.
echo ============================================
echo.
pause
