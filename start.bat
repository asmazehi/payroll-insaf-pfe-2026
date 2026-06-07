@echo off
echo Starting INSAF Platform...
echo.
echo Starting Ollama...
start "" ollama serve
timeout /t 3 /nobreak >nul

echo Starting all containers...
docker compose up -d --build

echo.
echo Done! Open http://localhost:80
pause
