## dbt-teradata 0.19.0 (TBD)

* Fixed `teradata__create_table_as` and `teradata__create_view_as` macros to accommodate models with CTE's and without CTE's.
* Added support for LOGMECH authentication parameter. With LOGMECH support, the adapter can now authenticate using LDAP, Kerberes, TDNEGO and the database native protocol.
* Eliminated the need for `dbt_drop_relation_if_exists` stored procedure
* Implemented `teradata__create_schema` and `teradata__drop_schema` macros
