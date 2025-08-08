@echo off
echo ⚡ FAST Loading Harmonic Pattern Scanner with Live Data
echo.

REM Check if Python is installed
python --version >nul 2>&1
if errorlevel 1 (
    echo ❌ Python is not installed or not in PATH
    echo Please install Python first
    pause
    exit
)

REM Install required packages quickly
echo 📦 Installing/updating required packages...
pip install yfinance pandas --quiet --upgrade

REM Start the live data server in background
echo 🚀 Starting live data server...
start /min "Live Data Server" python live_data_server.py

REM Wait for server to start
echo ⏳ Waiting for server to initialize...
timeout /t 2 /nobreak >nul

REM Start web server and open browser
echo 🌐 Starting web server and opening browser...
start http://localhost:8080/harmonic-patterns.html
python -m http.server 8080

echo.
echo 🛑 Press Ctrl+C to stop all servers
pause
