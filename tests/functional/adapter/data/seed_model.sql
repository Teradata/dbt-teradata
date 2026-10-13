-- No DROP TABLE here: this fixture runs once via project.run_sql_file() against a
-- freshly created, unique test schema, so on_model_hook cannot already exist. (The
-- `/*+ IF EXISTS */` hint only works through the adapter's own add_query() error
-- suppression, which run_sql_file() bypasses via a raw cursor.execute().)
-- MULTISET (not the Teradata default SET) so hooks that legitimately insert
-- byte-identical rows (e.g. project-level + model-level hooks firing in the
-- same invocation) aren't rejected with Error 2802 "Duplicate row error" --
-- matching Postgres, which allows duplicate rows by default.
CREATE MULTISET TABLE {schema}.on_model_hook (
    test_state       VARCHAR(20),
    target_dbname    VARCHAR(100),
    target_host      VARCHAR(100),
    target_name      VARCHAR(100),
    target_schema    VARCHAR(100),
    target_type      VARCHAR(100),
    target_user      VARCHAR(100),
    target_pass      VARCHAR(100),
    target_threads   INTEGER,
    run_started_at   VARCHAR(100),
    invocation_id    VARCHAR(100),
    thread_id        VARCHAR(100)
);
