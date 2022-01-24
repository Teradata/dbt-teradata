## dbt-teradata 1.0.0a

### Features
* Optimize the test query to use `SAMPLE` instead of `LIMIT` clause
* Fix timestamp format in teradata__curent_timestamp macro
* Use macro "information_schema_name(database)" instead of word "DBC" (https://github.com/Teradata/dbt-teradata/issues/19)
* Remove unnecessary word "distinct" in macro "teradata__list_schemas(database)" https://github.com/Teradata/dbt-teradata/issues/18

### Fixes

### Docs

### Under the hood
