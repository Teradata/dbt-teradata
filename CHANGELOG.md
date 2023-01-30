## dbt-teradata 1.2.0.0

### Features
* Upgrade to dbt v1.2.0 [#55](https://github.com/Teradata/dbt-teradata/pull/55)
* Added connection retries functionality with 'retries' configuration [#56](https://github.com/Teradata/dbt-teradata/pul1/56)
* Added grants feature [#59](https://github.com/Teradata/dbt-teradata/pul1/59)

### Fixes
* Setting `on_schema_change="append_new_columns"` or `on_schema_change="sync_all_columns"` in the model files will now append or sync all columns in the existing table. This fixes issue [#48](https://github.com/Teradata/dbt-teradata/issues/48). 

### Docs

### Under the hood
