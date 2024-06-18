## dbt-teradata 1.0.0a

### Features
* dbt-teradata is now compatible with dbt-core 1.8.x
* dbt-teradata adapter has been decoupled from dbt-core and is now compatible with dbt-adapters >= 1.2.1, dbt-common >= 1.3.0
* Unit test feature of dbt 1.8 is now supported in dbt-teradata
* Added support for --empty flag for dry run
### Fixes
* Fixed - Collisions in dbt_scd_id while calculating snapshots (https://github.com/Teradata/dbt-teradata/issues/160)
### Docs

### Under the hood
* Changes in workflow files for better release activity
