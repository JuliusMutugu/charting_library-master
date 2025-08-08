@echo off
echo 🚀 Starting Live Market Data Harmonic Pattern Scanner
echo.
echo 📊 Server 1: Live Data Server (Port 8083) - Yahoo Finance API
echo 🌐 Server 2: Web Server (Port 8080) - Chart Interface
echo.

REM Start the live data server in a new window
start "Live Data Server" cmd /k "cd /d "%~dp0" && python live_data_server.py"

REM Wait a moment for the data server to start
timeout /t 3 /nobreak >nul

REM Start the web server
echo Starting web server on port 8080...
python -m http.server 8080

pause
