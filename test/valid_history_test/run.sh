#!/usr/bin/env bash
set -e

dbt -d deps --target dbt_test_valid_history
dbt -d seed --target dbt_test_valid_history --full-refresh
dbt -d run --target dbt_test_valid_history
dbt -d run --target dbt_test_valid_history