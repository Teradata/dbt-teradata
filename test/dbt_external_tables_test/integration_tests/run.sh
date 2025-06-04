#!/usr/bin/env bash
set -e

dbt -d deps --target dbt_external_tables
dbt -d seed --target dbt_external_tables --full-refresh
dbt -d run-operation prep_external --target dbt_external_tables
dbt -d run-operation dbt_external_tables.stage_external_sources --target dbt_external_tables
dbt -d run-operation dbt_external_tables.stage_external_sources --vars 'ext_full_refresh: true' --target dbt_external_tables
dbt test --target dbt_external_tables