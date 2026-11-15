@echo off
title Suivi de la collecte IMDb
powershell.exe -NoProfile -ExecutionPolicy Bypass -File "%~dp0Watch-Enrichment.ps1"
echo.
pause
