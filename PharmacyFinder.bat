@echo off
title PharmacyFinder Dashboard
echo Starting PharmacyFinder Dashboard...
echo.

:: Start server in background
start /min "PharmacyFinder Server" python "%~dp0serve_dashboard.py"

:: Wait for server to be ready
timeout /t 2 /nobreak >nul

:: Open browser
start http://localhost:8050

echo Dashboard running at http://localhost:8050
echo Close this window to stop the server.
