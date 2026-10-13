## dbt-teradata 1.0.0a

### Features
- Audit columns: added `get_audit_columns()` for `job_id`, timezone-aware
  `job_start_tstmp`, and nullable timezone-aware `job_end_tstmp`, plus
  `audit_columns_post_hook()` to stamp model completion. Includes functional
  Teradata coverage and a runnable sample project.

- Role-aware grants (`teradata_grants_extended`): the `grants` config now
  supports `role:<name>` and `user:<name>` grantee prefixes, letting dbt
  manage Teradata's native ROLE-based access control idempotently.
  When `vars: {teradata_grants_extended: true}` is set in `dbt_project.yml`,
  `get_show_grant_sql` unions `DBC.AllRightsV` (user grants) with
  `DBC.AllRoleRightsV` (role grants), the diff engine sees both, and only the
  delta is applied on each run.  REVOKE statements automatically emit the
  `ROLE` keyword required by Teradata's DCL syntax.  Grantees are diffed in
  type-qualified form (`role:<name>` / `user:<name>`) on both sides — bare names
  in the config normalize to `user:<name>` — so grants stay idempotent whichever
  spelling is used.  Bare grantee names (no prefix) continue to work as
  before - full backward compatibility.
  [IDE-26229]

- `GetCatalogForSingleRelation` capability: dbt can now fetch catalog metadata
  (table type, columns, comments) for a single relation on demand via
  `DBC.TablesV` + `get_columns_in_relation`, instead of scanning an entire
  schema. This improves performance for `dbt docs generate` and state comparison
  on large Teradata databases. [IDE-26226]
- Python models (Beta, client-side): support `.py` models with `materialized='table'`.
  The model's Python runs in the dbt runner process; upstream `ref()`/`source()`
  relations are read into pandas DataFrames over `teradatasql` and the returned
  DataFrame is written back to the target table. Intended for small/medium models
  (data is materialized in the runner's memory). Supports multiple `ref()`s
  (pandas joins), mixed SQL+Python DAGs, `use_fastload`, and best-effort
  `teradataml` DataFrame returns (via `.to_pandas()`). See the "Python models
  (Beta)" section in the README for details. [IDE-24196]

### Fixes
- `valid_history` incremental strategy: fixed `[Error 9134] Normalize: The output
  grouping column list does not match with input grouping column list`, which
  occurred on the second and subsequent runs whenever a tracked column was a
  fixed-width `CHAR(n)` with a LATIN/default character set.
  `teradata__get_columns_in_relation` previously mapped both `CHAR` and
  `VARCHAR` catalog codes (`CF`/`CV`) to the same `CHARACTER` dtype label, and
  `TeradataColumn` had no override for `data_type`, so the base implementation
  always rendered `character varying(n)` — mismatching the physical `CHAR(n)`
  type declared in the generated `TD_NORMALIZE_MEET RETURNS` clause. `CV` now
  maps to `CHARACTER VARYING` and `TeradataColumn.data_type` renders
  `character(n)` for `CHAR` columns, so `data_type` correctly reflects
  fixed-width vs. variable-width sizing for LATIN/default-character-set
  columns (`string_type()` remains the inherited base classmethod, unchanged,
  to preserve `dbt-core`'s unbound-call contract in `expand_column_types()`).
  Known limitation: `CHARACTER SET` (e.g. `UNICODE`) is not represented in
  `data_type`/the generated `RETURNS` clause, so a `CHAR(n)` column declared
  `CHARACTER SET UNICODE` can still hit `Error 9134` on the incremental path.
  The same gap applies to the `KANJISJIS` and `GRAPHIC` server character sets:
  `teradata__get_columns_in_relation` derives `char_size` for these by halving
  the byte length (`DBC.ColumnsV.ColumnLength`), same as `UNICODE`, but
  `KANJISJIS` is a mixed 1-2 byte/character encoding, so this is a best-effort
  approximation rather than a guaranteed-exact recovery of the declared size;
  `GRAPHIC` is fixed 2 bytes/character so the halved value is exact. Neither
  character set is represented in the `RETURNS` clause, so both can still hit
  `Error 9134` on the incremental path regardless of `char_size` accuracy.
  [IDE-26634]
- Grants: `get_show_grant_sql`'s table/view read-back now queries
  `DBC.AllRightsV` instead of the legacy `DBC.AllRights` table. `AllRights`
  stores `DatabaseName` as `CHAR(30)`, silently truncating schema names longer
  than 30 characters; an exact-match lookup against a truncated name returned
  zero rows, so dbt's grant diff treated any existing grants on such schemas as
  "already absent" and never revoked a grantee removed from the `grants:`
  config. The function/UDF and role-based branches already used the
  un-truncated `DBC.AllRightsV`/`DBC.AllRoleRightsV` views; only the base
  table/view branch had the bug. [IDE-26643]
- Python models: write-back is now atomic. The result is built under a
  `<target>__dbt_tmp` staging table and only swapped into place (drop target +
  RENAME) once the load fully succeeds, mirroring the SQL table materialization.
  A failed or cancelled (Ctrl-C) load no longer leaves the target dropped or
  half-populated — the previous table is left intact. [IDE-24196]
- Python models: type-mapping/write-back robustness (found during manual test
  pass): object columns bind as strings so a mixed-type column no longer breaks
  the batch insert; strings longer than 32000 chars now map to CLOB (with
  NO PRIMARY INDEX) instead of a truncated VARCHAR that failed the load; and a
  DataFrame with duplicate column names now raises a clear error. [IDE-24196]
- `merge` strategy: fixed quoting/case-sensitivity mismatch that prevented `unique_key` columns from being excluded from the `SET` clause when using `merge_update_columns`, causing Teradata Error 5977 (cannot update a Primary Index column). Also fixed an empty `SET` list edge case that produced a bare `WHEN MATCHED THEN UPDATE SET` causing Teradata Error 3707 — a misconfigured `merge_update_columns`/`merge_exclude_columns` that leaves no updatable columns now raises a clear compiler error instead of silently degrading to insert-only behavior; the `WHEN MATCHED` clause is only omitted (with a log) when the model genuinely has no non-key columns.

### Docs
- Added a "Python models (Beta)" section to the README (how it works, supported /
  not-supported, configuration, examples, limitations). [IDE-24196]
- Added a runnable sample project `test/python_model_sample_project` covering
  every supported scenario. [IDE-24196]

### Under the hood
- Expanded Python-model test coverage: unit suite for submission wiring, type
  mapping, DDL/binding generation, the atomic stage-and-swap sequence and its
  cleanup-on-failure; functional suite (live instance) for the happy-path
  scenarios, empty/FastLoad write-back, idempotent re-run, write-failure
  durability, and materialization guardrails. [IDE-24196]
