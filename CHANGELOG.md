## dbt-teradata 1.10.0a

### Features
* Upgraded dbt-teradata adapter to support dbt-core v1.10 (Python 3.13, new flags, and deprecated function replacements).

### Fixes
* Fixed unit test failures by updating dbt-teradata adapter to align with new macros and changes introduced in dbt-core.
* Fixed column misalignment in snapshot helpers.sql by ensuring consistent column ordering across union-all statements.
  * [221](https://github.com/Teradata/dbt-teradata/issues/221)
* Fix for teradata__get_columns_in_relation macro do not respect fallback_schema
  * [218](https://github.com/Teradata/dbt-teradata/issues/218)
  * [220](https://github.com/Teradata/dbt-teradata/issues/220)
* Snapshot timestamp strategy ignoring user-defined hashing function set via snapshot_hash_udf config.
  * [222](https://github.com/Teradata/dbt-teradata/issues/222)

### Docs

### Under the hood
* Changed to config name from 'fallback_schema' to 'temporary_metadata_generation_schema'
