## dbt-teradata 1.3.3.1

### Features
* Merge incremental strategy is now supported in dbt-teradata

### Fixes
* Fixed the bug (https://github.com/Teradata/dbt-teradata/issues/77), this is related to change in column type in source.
    
### Docs

### Under the hood
* Fixed issue regarding temp tables not getting dropped if incremental materialization fails
* Functional Testcases have been added