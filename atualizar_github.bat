@echo off
cd /d "%~dp0"
git add .
git commit -m "Atualização automática - %date% %time:~0,5%"
git push origin main
pause
