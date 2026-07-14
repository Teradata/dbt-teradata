## dbt-teradata 1.0.0a

### Features
* Added support for the dbt `function` resource type (dbt 1.11+), enabling `REPLACE FUNCTION` materialization of Teradata SQL scalar UDFs. Aggregate UDFs (`type: aggregate`) raise a clear compile-time error since they are not supported.
* Implement `persist_docs` support ([IDE-26225](https://teradata-pe.atlassian.net/browse/IDE-26225)): model and column `description:` text is now written to the Teradata catalog as native `COMMENT ON TABLE/VIEW/FUNCTION/COLUMN` metadata for table, view, incremental, seed, and snapshot materializations; for `function` materializations, only the relation-level `description:` is applied via `COMMENT ON FUNCTION` (argument/column-level docs are not supported). Comments are escaped, capped at Teradata's 255-character limit, only re-issued when changed, and skipped gracefully for OTF/Iceberg relations. Relation comments now also surface in `dbt docs generate` catalog output for relational resources (tables/views/etc.); dbt-core excludes `function` nodes from the catalog fetch, so function comments do not appear there.

### Fixes
* Fixed snapshots with `hard_deletes='new_record'` reprocessing already-deleted records on every subsequent run, generating a new "deletion" record (with a reused `dbt_scd_id`) each time instead of only once when the deletion is first detected [238](https://github.com/Teradata/dbt-teradata/issues/238)

### Docs

### Under the hood
