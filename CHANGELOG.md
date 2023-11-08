## dbt-teradata 1.0.0a

### Features
* dbt-teradata 1.6.7 is now compatible with dbt-core 1.6.7

### Fixes
* Below issues fixed related to cross DB macros 
    1. https://github.com/Teradata/dbt-teradata/issues/105 
    2. https://github.com/Teradata/dbt-teradata/issues/102

* Reconfiguration of dbt-teradata snapshot code for fixing the below issue
    https://github.com/Teradata/dbt-teradata/issues/65
    
### Docs
* Stubbed model contracts support
* materialized_view is not yet supported

### Under the hood
* Addition of some more adapter zone testcases