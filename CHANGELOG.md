## dbt-teradata 0.19.0 (TBD)

### Features
* Added support for LOGMECH authentication parameter. With LOGMECH support, the adapter can now authenticate using LDAP, Kerberes, TDNEGO and the database native protocol ([#2](https://github.com/dbeatty10/dbt-teradata/pull/2))
* Incremental materialization ([#16](https://github.com/dbeatty10/dbt-teradata/issues/16), [#17](https://github.com/dbeatty10/dbt-teradata/pull/17))
* Eliminated the need for `dbt_drop_relation_if_exists` stored procedure ([#5](https://github.com/dbeatty10/dbt-teradata/pull/5)
* Implemented `teradata__create_schema` and `teradata__drop_schema` macros ([#5](https://github.com/dbeatty10/dbt-teradata/pull/5)
* Added support for remaining connection parameters ([#22](https://github.com/dbeatty10/dbt-teradata/issues/22))

### Fixes
* Enforce the max batch size of 2536 for seeds ([#4](https://github.com/dbeatty10/dbt-teradata/issues/4), [#11](https://github.com/dbeatty10/dbt-teradata/pull/11))
* Use CHARACTER as the default column type ([#7](https://github.com/dbeatty10/dbt-teradata/issues/7), [#9](https://github.com/dbeatty10/dbt-teradata/pull/9))
* Fixed `teradata__create_table_as` and `teradata__create_view_as` macros to accommodate models with CTE's and without CTE's.

### Docs
* LICENSE, CONTRIBUTING, RELEASE, and CHANGELOG files
* Instructions for running integration tests ([#6](https://github.com/dbeatty10/dbt-teradata/issues/6), [#8](https://github.com/dbeatty10/dbt-teradata/pull/8))

### Under the hood
* pytest-dbt-adapter integration tests ([#6](https://github.com/dbeatty10/dbt-teradata/issues/6), [#8](https://github.com/dbeatty10/dbt-teradata/pull/8))
* Integration tests in GitHub Actions ([#10](https://github.com/dbeatty10/dbt-teradata/issues/10), [#19](https://github.com/dbeatty10/dbt-teradata/pull/19), [#24](https://github.com/dbeatty10/dbt-teradata/issues/24), [#23](https://github.com/dbeatty10/dbt-teradata/pull/23))
* Introduced randomized database name in tests
