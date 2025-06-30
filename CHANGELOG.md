## dbt-teradata 1.0.0a

### Features
* Added support for dbt-external-tables [146](https://github.com/Teradata/dbt-teradata/issues/146)
  
### Fixes
* Added fallback_schema for metadata table creation when QVCI is disabled, also fixed the concurrency issues related to it
    * [212](https://github.com/Teradata/dbt-teradata/issues/212)
    * [213](https://github.com/Teradata/dbt-teradata/issues/213)

### Docs
* Addition of documentation for usage of dbt-external-tables and fallback_schema to Readme.
  
### Under the hood
* Enhanced exception messages with more information for better debugging.
