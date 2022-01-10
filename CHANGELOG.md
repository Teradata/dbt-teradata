## dbt-teradata 0.19.0.5

### Features
* When the adapter is not able to detect a column type, it will return `N/A` instead of defaulting to `CHARACTER`.
### Fixes
* Resolved [Populate column types in dbt docs](https://github.com/Teradata/dbt-teradata/issues/4)
### Docs

### Under the hood
* Added automated tests for catalog generation.
