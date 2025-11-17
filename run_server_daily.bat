@echo off
REM Change to the script's directory (project root)
cd /d "C:\Users\MagicTouch\Desktop\Nick\repos\daily_eff_oldway"

REM Run the Python script using uv
powershell.exe -Command "uv run python -m src.main"

pause