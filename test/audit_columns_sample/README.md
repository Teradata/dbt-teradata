# dbt-teradata Audit Columns Sample

This project demonstrates the `get_audit_columns()` and
`audit_columns_post_hook()` macros supplied by `dbt-teradata`.

Copy `profiles.yml.example` to a dbt profiles directory and replace its
connection values. Then run:

```bash
dbt build --profiles-dir .
```

The `audit_columns_example` table contains one row with these fields:

- `job_id`: the dbt invocation UUID.
- `job_start_tstmp`: the timezone-aware time dbt started this invocation. dbt
	supplies this in UTC.
- `job_end_tstmp`: the timezone-aware time this model completed.

`get_audit_columns()` supplies expressions for the model's `SELECT`; it sets
`job_end_tstmp` to `NULL` because the model has not finished yet. The
`post_hook="{{ audit_columns_post_hook() }}"` configuration is what tells dbt to
run the update after the model materializes. Configure that hook once at project
scope in `dbt_project.yml` when the project adopts audit columns. That update stamps
`job_end_tstmp` with Teradata's UTC `CURRENT_TIMESTAMP(6) AT TIME ZONE 'GMT'`
for rows from the current dbt invocation.

Both timestamps are stored in UTC. `job_start_tstmp` uses the dbt runner's
clock while `job_end_tstmp` uses the Teradata server's clock, so ordering them
assumes those hosts are time-synchronized.

Use the post-hook only with native Teradata `table` or `incremental` models.
When it is configured project-wide, views, snapshots, seeds, and
`catalog_name`/OTF models skip the completion update and emit an informational
dbt log entry. Native table/incremental models that do not call
`get_audit_columns()` also skip the update safely.

> **Required pair:** Always configure `post_hook="{{ audit_columns_post_hook() }}"`
> on a model that calls `get_audit_columns()`. Without the post-hook, dbt builds
> the model normally but leaves `job_end_tstmp` as `NULL` for every row.

For large incremental tables, consider a non-unique secondary index on `job_id`.
The completion hook filters by that column; without an appropriate access path,
Teradata may scan historical rows on every run. See the main adapter README for
an index configuration example and its storage/maintenance trade-off.
