## dbt-teradata 1.6.7a

### Features
* dbt-teradata 1.6.7 is now compatible with dbt-core 1.6.7

### Fixes
* Cross DB macros were not bundled in the dbt-teradata in earlier release, in this release they have been added to package.
  Below issues fixed related to cross DB macros
    1. https://github.com/Teradata/dbt-teradata/issues/105 
    2. https://github.com/Teradata/dbt-teradata/issues/102

* Reconfiguration of dbt-teradata snapshot code for fixing the issue related to invalidate_hard_deletes configuration not working
    https://github.com/Teradata/dbt-teradata/issues/65

### Docs
* Stubbed model contracts support
* materialized_view is not yet supported

### Under the hood
* Addition of some more adapter zone testcases