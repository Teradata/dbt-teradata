## dbt-teradata 1.7.0a

### Features
* dbt-teradata 1.7.0 is now compatible with dbt-core 1.7.3

* New capability support structure for adapters to include 
  TableLastModifiedMetadata and SchemaMetadataByRelations capabilities

* Catalog fetch performance improvements
    https://github.com/dbt-labs/dbt-core/issues/8521

* Added Metadata freshness check feature
    https://github.com/dbt-labs/dbt-core/issues/8704

* Migrated date_spine() and dependent macros
    https://github.com/dbt-labs/dbt-core/issues/8172

### Fixes
* Fixed issue related to dbt show with limit option
    https://github.com/Teradata/dbt-teradata/issues/125

### Docs
* Stubbed model contracts support
* materialized_view is not yet supported

### Under the hood
* Addition of some more adapter zone testcases
