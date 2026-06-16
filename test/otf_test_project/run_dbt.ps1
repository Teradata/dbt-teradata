# dbt-teradata OTF Test Project Runner
# This script sets up environment variables, creates prerequisites, and executes dbt commands
# Usage: .\run_dbt.ps1 [dbt-command]
# Examples:
#   .\run_dbt.ps1 seed         # Load seed data and create raw tables
#   .\run_dbt.ps1 run
#   .\run_dbt.ps1 "run --select otf_orders"
#   .\run_dbt.ps1 debug
#   .\run_dbt.ps1 test
#   .\run_dbt.ps1 "run --threads 4"
#   .\run_dbt.ps1 "setup"      # Initialize raw database (requires teradatasql)

param(
    [string]$Command = "run",
    [string]$ServerName = "pe18-tdbluesky-0004",
    [string]$Username = "mt255026",
    [string]$Password = "mt255026",
    [string]$Schema = "mt255026",
    [string]$DataLake = "MyOTFLake",
    [string]$OTFDatabase = "otf_test_db",
    [int]$Threads = 4,
    [switch]$RunSetupFirst = $false
)

Write-Host "========================================" -ForegroundColor Cyan
Write-Host "dbt-teradata OTF Test Project" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""

Write-Host "Configuration:" -ForegroundColor Yellow
Write-Host "  Server:       $ServerName"
Write-Host "  Username:     $Username"
Write-Host "  Schema:       $Schema"
Write-Host "  DataLake:     $DataLake"
Write-Host "  OTF Database: $OTFDatabase"
Write-Host "  Threads:      $Threads"
Write-Host "  Command:      $Command"
Write-Host ""

# Set environment variables
Write-Host "Setting environment variables..." -ForegroundColor Green
$env:DBT_TERADATA_SERVER_NAME = $ServerName
$env:DBT_TERADATA_USERNAME = $Username
$env:DBT_TERADATA_PASSWORD = $Password
$env:DBT_TERADATA_SCHEMA = $Schema
$env:DBT_TERADATA_DATALAKE = $DataLake
$env:DBT_TERADATA_OTF_DATABASE = $OTFDatabase

Write-Host "✓ Environment variables set" -ForegroundColor Green
Write-Host ""

# Automatically run dbt seed before other commands if needed
$needsSetup = @("run", "test", "build", "compile") -contains $Command
if ($needsSetup -and -not $RunSetupFirst) {
    Write-Host "Pre-running: dbt seed (to ensure raw tables exist)" -ForegroundColor Yellow
    Write-Host "========================================" -ForegroundColor Cyan
    dbt seed --profiles-dir .
    Write-Host "✓ Seed data loaded" -ForegroundColor Green
    Write-Host ""
}

# Execute dbt command
Write-Host "Executing: dbt $Command --profiles-dir . --threads $Threads" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""

# Add threads parameter if not already in command and running a build command
if ($Command -notmatch "--threads" -and ($Command -like "run*" -or $Command -like "test*" -or $Command -like "build*")) {
    dbt $Command --profiles-dir . --threads $Threads
} else {
    dbt $Command --profiles-dir .
}

$exitCode = $LASTEXITCODE
Write-Host ""
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "Execution Complete" -ForegroundColor Cyan
Write-Host "Exit Code: $exitCode" -ForegroundColor $(if ($exitCode -eq 0) { 'Green' } else { 'Red' })
Write-Host "========================================" -ForegroundColor Cyan

exit $exitCode
