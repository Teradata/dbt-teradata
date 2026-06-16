@echo off
REM dbt-teradata OTF Test Project Runner (Windows CMD)
REM Usage: run_dbt.bat [dbt-command] [threads]
REM Examples:
REM   run_dbt.bat run
REM   run_dbt.bat "run --select otf_orders"
REM   run_dbt.bat debug
REM   run_dbt.bat test

setlocal enabledelayedexpansion

REM Get parameters
set "DBT_COMMAND=%1"
if "%DBT_COMMAND%"=="" set "DBT_COMMAND=run"

set "THREADS=%2"
if "%THREADS%"=="" set "THREADS=4"

REM Set environment variables for Teradata connection
set "DBT_TERADATA_SERVER_NAME=pe18-tdbluesky-0004"
set "DBT_TERADATA_USERNAME=mt255026"
set "DBT_TERADATA_PASSWORD=mt255026"
set "DBT_TERADATA_SCHEMA=mt255026"
set "DBT_TERADATA_DATALAKE=MyOTFLake"
set "DBT_TERADATA_OTF_DATABASE=otf_test_db"

echo ========================================
echo dbt-teradata OTF Test Project
echo ========================================
echo.
echo Configuration:
echo   Server:       %DBT_TERADATA_SERVER_NAME%
echo   Username:     %DBT_TERADATA_USERNAME%
echo   Schema:       %DBT_TERADATA_SCHEMA%
echo   DataLake:     %DBT_TERADATA_DATALAKE%
echo   OTF Database: %DBT_TERADATA_OTF_DATABASE%
echo   Threads:      %THREADS%
echo   Command:      %DBT_COMMAND%
echo.

echo Setting environment variables...
echo   DBT_TERADATA_SERVER_NAME=%DBT_TERADATA_SERVER_NAME%
echo   DBT_TERADATA_USERNAME=%DBT_TERADATA_USERNAME%
echo   DBT_TERADATA_SCHEMA=%DBT_TERADATA_SCHEMA%
echo   DBT_TERADATA_DATALAKE=%DBT_TERADATA_DATALAKE%
echo   DBT_TERADATA_OTF_DATABASE=%DBT_TERADATA_OTF_DATABASE%
echo.

REM Auto-run dbt seed before run/test/build if not already running seed
if "%DBT_COMMAND%"=="run" (
    echo Pre-running: dbt seed (to ensure raw tables exist)
    echo ========================================
    dbt seed --profiles-dir .
    echo.
)

echo Executing: dbt %DBT_COMMAND% --profiles-dir . --threads %THREADS%
echo ========================================
echo.

dbt %DBT_COMMAND% --profiles-dir . --threads %THREADS%

set EXIT_CODE=%ERRORLEVEL%

echo.
echo ========================================
echo Execution Complete
echo Exit Code: %EXIT_CODE%
echo ========================================

exit /b %EXIT_CODE%
