dbt-external-tables is based on https://github.com/dbt-labs/dbt-external-tables

Testing instructions:
1. create a teradata dbt profile called integration_tests
2. create python venv with dbt-teradata
3. change directory to  integration_tests
4. run make -f ../Makefile test-all