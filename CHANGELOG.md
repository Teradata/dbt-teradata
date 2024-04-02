## dbt-teradata 1.0.0a

### Features
* dbt-teradata 1.7.2 is now compatible with dbt-core 1.7.11
* Adding the native JSON datatype support 
  * https://github.com/Teradata/dbt-teradata/issues/142

### Fixes
* Fixed the issue with rendering of index creation from configs
  * https://github.com/Teradata/dbt-teradata/issues/144
* Fixed the issue with run_result.json has rows_affected = 0 always when data are inserted into target table, and code shows success instead of insert or update
  * https://github.com/Teradata/dbt-teradata/issues/145


### Under the hood
* No security vulnerabilities reported in the Snyk report
