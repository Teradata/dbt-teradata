## dbt-teradata 1.0.0a

### Features
- Open Table Format (OTF) support via Teradata DATALAKE objects. Models can target Iceberg/Delta Lake tables using 3-part naming (`<datalake>."<otf_db>"."<table>"`) by setting `catalog_name` and registering a `datalake` catalog integration in `catalogs.yml`. Supports model-level `partitioned_by`, `sorted_by`, `tblproperties`, and `purge_mode` configs. OTF tables defined as sources in `sources.yml` (with differing `database` and `schema`) are auto-detected and rendered with 3-part naming.
- `grants`, `persist_docs`, and adapter cache management are now applied to OTF table materializations, matching the standard table materialization.

### Fixes
- `purge_mode` default changed from `PURGE ALL` to `NO PURGE` to avoid data loss on first DROP of an OTF model whose target table already holds data.
- `purge_mode` validation is now case-insensitive and centralised; invalid values raise a clear compile-time error.
- `add_query` now matches `DROP/DELETE DATABASE` case-insensitively, consistent with the `DROP TABLE/VIEW` branch.

### Docs

### Under the hood
- Adapter suppresses Teradata error 7825 ("OTF table not found in external catalog") on `DROP TABLE /*+ IF EXISTS */`, matching the existing semantics for errors 3807/3853/3854.
- OTF DROP logic deduplicated across `teradata__drop_relation` and `teradata__create_datalake_table_as` via shared `teradata__drop_otf_table` / `teradata__validate_purge_mode` helper macros.
- Unsupported config combinations on the OTF path (`table_kind`, `table_option`, `with_statistics`, `index`, `contract.enforced`) now raise compile-time errors instead of being silently ignored.
- `TeradataRelation.render()` for OTF relations no longer relies on the private `BaseRelation._render_iterator()` API.
