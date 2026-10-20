@echo off
setlocal enabledelayedexpansion
title E-Commerce Customer Segmentation Dashboard

rem ============================================================
rem  Quick-start launcher (Windows)
rem  Usage:
rem      start.bat          -> development server (Flask, auto-reload)
rem      start.bat prod     -> production server (Waitress)
rem  Just double-click this file to launch the app and open a browser.
rem ============================================================

set "ROOT=%~dp0"
set "BACKEND=%ROOT%backend"
set "HOST=127.0.0.1"
set "PORT=5000"
set "MODE=%~1"

echo ============================================================
echo   E-Commerce Customer Segmentation Dashboard
echo ============================================================
echo.

rem --- Locate Python -------------------------------------------------
where python >nul 2>nul
if errorlevel 1 (
    echo [ERROR] Python was not found on your PATH.
    echo         Install Python 3.11+ from https://www.python.org
    echo         and tick "Add Python to PATH", then run this again.
    echo.
    pause
    exit /b 1
)

cd /d "%BACKEND%"
if errorlevel 1 (
    echo [ERROR] Could not enter "%BACKEND%".
    pause
    exit /b 1
)

rem --- Ensure dependencies are installed -----------------------------
rem Flask is the sentinel: if it imports, the environment is ready.
python -c "import flask" >nul 2>nul
if errorlevel 1 (
    echo [SETUP] Dependencies not detected. Setting up a virtual environment...
    echo         This installs Flask, Pandas, PySpark, Waitress and friends and
    echo         may take several minutes the first time.
    echo.
    python -m venv "%ROOT%.venv"
    call "%ROOT%.venv\Scripts\activate.bat"
    python -m pip install --upgrade pip >nul
    pip install -r requirements.txt
    if errorlevel 1 (
        echo.
        echo [ERROR] Dependency installation failed. Check the messages above.
        echo         Note: PySpark also requires Java 11+ on your PATH.
        pause
        exit /b 1
    )
    echo [OK] Dependencies installed.
) else (
    echo [OK] Python environment ready.
)

rem --- Launch --------------------------------------------------------
set "FLASK_RUN_HOST=%HOST%"
set "FLASK_RUN_PORT=%PORT%"

echo.
echo Opening http://%HOST%:%PORT% in your browser shortly...
echo Press Ctrl+C in this window to stop the server.
echo ------------------------------------------------------------
start "Opening browser" cmd /c "timeout /t 3 >nul & explorer http://%HOST%:%PORT%"

if /i "%MODE%"=="prod" (
    set "FLASK_ENV=production"
    echo [RUN] Starting production server - Waitress - on http://%HOST%:%PORT%
    python serve.py
) else (
    echo [RUN] Starting development server - Flask - on http://%HOST%:%PORT%
    python run.py
)

echo.
echo Server stopped.
pause
endlocal
