## dbt-teradata 1.0.0a

### Features
* Model contracts are now supported with dbt-teradata v1.7.1 and onwards
* Addition of Tera Mode in dbt-teradata
    ###### IMPORTANT NOTE: This is an initial implementation of the TERA transaction mode and may not support some use cases. We strongly advise validating all records or transformations utilizing this mode to preempt any potential anomalies or errors.

### Fixes
* Fixed the bug related to missing keyword 'raise' in one of the exception handling 
 https://github.com/Teradata/dbt-teradata/issues/133
* Fixed the Issue: Incremental Materialization : Merge not working when update does single column
  https://github.com/Teradata/dbt-teradata/issues/120

### Docs

### Under the hood
* Migrated CI pipeline to environments dynamically provisioned in ClearScape Analytics Experience (https://clearscape.teradata.com/)
