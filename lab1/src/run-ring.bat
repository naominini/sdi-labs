@echo off
set ROUNDS=%1
if "%ROUNDS%"=="" set ROUNDS=3
cd /d "%~dp0"
start "P2" java RingNode.java P2 6002 localhost 6003
start "P3" java RingNode.java P3 6003 localhost 6001
timeout /t 2 /nobreak >nul
start "P1" java RingNode.java P1 6001 localhost 6002 --start %ROUNDS%