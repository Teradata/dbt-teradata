## dbt-teradata 1.7.3a

### Features
* dbt-teradata 1.7.3 is now compatible with dbt-core 1.7.16
* Adding 'valid_history' incremental materialization strategy (early access)
* Implementation of query_band support in dbt-teradata

### Fixes
* Fixed the issue with expanding the size of Unicode varchars.
  * https://github.com/Teradata/dbt-teradata/issues/153
* Fixed the issue where the 'TeradataCursor' object lacked the 'activityname' attribute.
  * https://github.com/Teradata/dbt-teradata/issues/155
* Fixed the model encountering the "Syntax error: Multiple definitions for WITH" error.
  * https://github.com/Teradata/dbt-teradata/issues/161
