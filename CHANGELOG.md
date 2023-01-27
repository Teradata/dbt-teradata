## dbt-teradata 1.2.0a

### Features

### Fixes
* Setting `on_schema_change="append_new_columns"` or `on_schema_change="sync_all_columns"` in the model files will now append or sync all columns in the existing table. This fixes issue [#48](https://github.com/Teradata/dbt-teradata/issues/48). 

### Docs

### Under the hood
