## dbt-teradata 1.8.2a

### Features
* Addition of md5_udf variable for custom hash macro configuration
  * (https://github.com/Teradata/dbt-teradata-utils/issues/2)
* Full support for valid_history incremental strategy
* Remove support and testing for Python 3.8, which is now EOL

### Fixes
* Adapter does not resolve dates correctly in unit testing
* Snapshots fail on structure changes
  * (https://github.com/Teradata/dbt-teradata/issues/192)

### Docs
Updated Readme for 
* md5_udf variable - for custom hash macro configuration
* valid_history incremental strategy
* unit testing support

### Under the hood
* Test project for valid_history incremental strategy
* Addition of more function tests for better coverage
* Change in workflow file for testing md5_udf variable
