# Testing dbt-teradata

## Overview

Here are the steps to run the integration tests:
1. Set environment variables
1. Run tests

## Simple example

Assuming the applicable `pytest-dbt-adapter` package is installed and environment variables are set:
```bash
pytest test/integration/teradata-17.10.dbtspec
```

## Full example

### Prerequisites
- [`pytest-dbt-adapter`](https://github.com/fishtown-analytics/dbt-adapter-tests) package

### Environment variables

Create the following environment variables (e.g., `export {VARIABLE}={value}` in a bash shell or via a tool like [`direnv`](https://direnv.net/)):
    * `DBT_TERADATA_SERVER_NAME`
    * `DBT_TERADATA_USERNAME`
    * `DBT_TERADATA_PASSWORD`

### Run tests

Run the test specs in this repository (with verbose output):
```
pytest -v test/integration/teradata-17.10.dbtspec
```
