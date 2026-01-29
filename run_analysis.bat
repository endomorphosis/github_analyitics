@echo off
REM GitHub Repository Analyzer - Windows Batch Script
REM Uses gh CLI for authentication

echo ======================================================================
echo GitHub Repository Analyzer
echo ======================================================================
echo.

REM Check if gh CLI is available
where gh >nul 2>&1
if %ERRORLEVEL% NEQ 0 (
    echo Error: gh CLI is not installed or not in PATH
    echo Please install from: https://cli.github.com/
    pause
    exit /b 1
)

REM Check if Python is available
where python >nul 2>&1
if %ERRORLEVEL% NEQ 0 (
    echo Error: Python is not installed or not in PATH
    echo Please install from: https://www.python.org/
    pause
    exit /b 1
)

echo Running clone_and_analyze (package module)...
echo.

python -m github_analyitics.reporting.clone_and_analyze

pause
