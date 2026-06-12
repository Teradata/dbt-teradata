## dbt-teradata 1.0.0a

### Features
- Open Table Format (OTF) support via Teradata DATALAKE objects. Models can target Iceberg/Delta Lake tables using 3-part naming (`"<datalake>"."<otf_db>"."<table>"`) by setting `catalog_name` and registering a `datalake` catalog integration in `catalogs.yml`. Supports model-level `partitioned_by`, `sorted_by`, `tblproperties`, and `purge_mode` configs. OTF tables defined as sources in `sources.yml` (with differing `database` and `schema`) are auto-detected and rendered with 3-part naming.
- `persist_docs` and adapter cache management are now applied to OTF table materializations, matching the standard table materialization. `grants` remain unsupported for OTF tables and are warned about and ignored.
- OTF incremental models now support dbt's `on_schema_change` with `'fail'` and `'append_new_columns'` (in addition to the default `'ignore'`). `'append_new_columns'` detects new source columns, issues one `ALTER TABLE ... ADD` per column, and back-fills `NULL` for pre-existing rows; the positional insert is reordered to match the resulting OTF column layout. `'sync_all_columns'` is intentionally not supported on OTF (it must synchronize column type changes, but OTF/Iceberg permits only a narrow set of type promotions — e.g. `VARCHAR` length changes fail — and it performs destructive, irreversible column drops) and raises a compile-time error; use `--full-refresh` instead.

### Fixes
- `purge_mode` default changed from `PURGE ALL` to `NO PURGE` to avoid data loss on first DROP of an OTF model whose target table already holds data.
- `purge_mode` validation is now case-insensitive and centralised; invalid values raise a clear compile-time error.
- `add_query` now matches `DROP/DELETE DATABASE` case-insensitively, consistent with the `DROP TABLE/VIEW` branch.
- OTF existence checks (`otf_relation_exists`) and `DROP TABLE /*+ IF EXISTS */` now treat Teradata error 6321 ("OTF Error: Table does not exist", raised by newer OTF engines such as 20.0.0.61) as "not found", in addition to 7825 — fixing first-run failures of OTF incremental models on those engines.

### Docs
- Documented OTF incremental materialization (`append` strategy) and `on_schema_change` support — including the supported values, behavior, and limitations — in the README.

### Under the hood
- Adapter suppresses Teradata error 7825 ("OTF table not found in external catalog") on `DROP TABLE /*+ IF EXISTS */`, matching the existing semantics for errors 3807/3853/3854.
- Added `TeradataAdapter.get_otf_columns_in_relation()`, which reads an OTF table's column names from `SELECT ... SAMPLE 0` result metadata (OTF tables are not registered in `DBC.ColumnsV`); used by `on_schema_change` reconciliation.
- OTF DROP logic deduplicated across `teradata__drop_relation` and `teradata__create_datalake_table_as` via shared `teradata__drop_otf_table` / `teradata__validate_purge_mode` helper macros.
- Unsupported config combinations on the OTF path (`table_kind`, `table_option`, `with_statistics`, `index`, `contract.enforced`) now raise compile-time errors instead of being silently ignored.
- `TeradataRelation.render()` for OTF relations no longer relies on the private `BaseRelation._render_iterator()` API.
