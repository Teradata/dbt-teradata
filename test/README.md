# Testing dbt-teradata

## Overview

Here are the steps to run the integration tests:
1. Setup test database
1. Install python package dependencies
1. Set environment variables
1. Run tests

## Simple example

Assuming the applicable `pytest-dbt-adapter` package is installed and environment variables are set:
```bash
pytest test/integration/teradata-17.10.dbtspec
```

## Full example

### Setup Test database
Manually run this SQL script in your database (edit it first if the user has a different name than `dbc`):
- `script/test_setup.sql`

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
pytest -v test/integration/teradata-17.10.dbtspec
```

### Enable/disable individual tests

Some tests that are expected to fail are commented out within this file:
- `test/integration/teradata-17.10.dbtspec`

Un-comment to test new functionality being added.

### Troubleshooting

`dbt/adapters/teradata/impl.py` contains the following Python class method definitions:
- `create_schema`
- `drop_schema`

Alternatively, these definitions can be removed and replaced with Jinja macro definitions within `dbt/include/teradata/macros/adapters.sql`:
- `teradata__create_schema`
- `teradata__drop_schema`

Currently, none of these are implemented. In order to pass the integration tests, this code within `impl.py` needs to be commented out:
```python
   def drop_schema(self, relation: BaseRelation):
       """Drop the given schema (and everything in it) if it exists."""
       # raise dbt.exceptions.NotImplementedException(
       #     f'`drop_schema` is not implemented for this adapter. Contact your Teradata administrator to `drop database {relation.without_identifier()};`'
       # )
       pass
```
