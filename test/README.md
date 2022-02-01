# Testing dbt-teradata

## Overview

Here are the steps to run the integration tests:
1. Setup Teradata server
1. Setup test database
1. Install python package dependencies
1. Set environment variables
1. Run tests

## Simple example

Assuming the applicable `pytest-dbt-adapter` package is installed and environment variables are set:
```bash
pytest test/integration
```

## Full example

### Setup Teradata server
Use one of the following methods to setup a test server:
- [Run Vantage Express on VirtualBox](https://quickstarts.teradata.com/docs/17.10/getting.started.vbox.html)
- [Run Vantage Express on VMware](https://quickstarts.teradata.com/docs/17.10/getting.started.vmware.html)

### Install test dependencies
```shell
python3 -m venv env
source env/bin/activate
pip install -r requirements_dev.txt
```

### Environment variables

Create the following environment variables (e.g., `export {VARIABLE}={value}` in a bash shell or via a tool like [`direnv`](https://direnv.net/)):
   * `DBT_TERADATA_SERVER_NAME`
   * `DBT_TERADATA_USERNAME`
   * `DBT_TERADATA_PASSWORD`

For example:
```shell
export DBT_TERADATA_SERVER_NAME='localhost'
export DBT_TERADATA_USERNAME='dbc'
export DBT_TERADATA_PASSWORD='dbc'
```

### Run tests

Run the test specs in this repository (with verbose output):
```
pytest -v test/integration
```

### Enable/disable individual tests

Some tests that are expected to fail are commented out within this file:
- `test/integration`

Un-comment to test new functionality being added.
