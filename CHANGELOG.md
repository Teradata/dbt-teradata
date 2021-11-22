## dbt-teradata 0.19.0 (TBD)

### Features
* Added support for LOGMECH authentication parameter. With LOGMECH support, the adapter can now authenticate using LDAP, Kerberes, TDNEGO and the database native protocol ([#2](https://github.com/dbeatty10/dbt-teradata/pull/2))
* Incremental materialization ([#16](https://github.com/dbeatty10/dbt-teradata/issues/16), [#17](https://github.com/dbeatty10/dbt-teradata/pull/17))

### Fixes
* Enforce the max batch size of 2536 for seeds ([#4](https://github.com/dbeatty10/dbt-teradata/issues/4), [#11](https://github.com/dbeatty10/dbt-teradata/pull/11))
* Use CHARACTER as the default column type ([#7](https://github.com/dbeatty10/dbt-teradata/issues/7), [#9](https://github.com/dbeatty10/dbt-teradata/pull/9))

### Docs
* LICENSE, CONTRIBUTING, RELEASE, and CHANGELOG files
* Instructions for running integration tests ([#6](https://github.com/dbeatty10/dbt-teradata/issues/6), [#8](https://github.com/dbeatty10/dbt-teradata/pull/8))

### Under the hood
* pytest-dbt-adapter integration tests ([#6](https://github.com/dbeatty10/dbt-teradata/issues/6), [#8](https://github.com/dbeatty10/dbt-teradata/pull/8))
