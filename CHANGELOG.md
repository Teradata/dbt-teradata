## dbt-teradata 1.9.0a

### Features
* Addition of new Snapshot features with dbt-teradata v1.9 ([#207](https://github.com/Teradata/dbt-teradata/pull/207)):
    * Enable setting a datetime value for dbt_valid_to for current records instead of NULL
    * Enable hard_deletes config to add a metadata column if a record has been deleted
    * Allow customizing names of metadata fields
    * Enable unique_key as a list

### Fixes
Fixes the exception handling on the valid_history incremental strategy

### Docs

### Under the hood
* Addition of testcases for Snapshot
