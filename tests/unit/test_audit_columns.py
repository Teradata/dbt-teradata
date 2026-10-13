from pathlib import Path


_MACRO_PATH = (
    Path(__file__).parents[2]
    / "dbt"
    / "include"
    / "teradata"
    / "macros"
    / "get_audit_columns.sql"
)


def test_audit_macro_exposes_timezone_aware_job_fields() -> None:
    macro = _MACRO_PATH.read_text(encoding="utf-8")

    assert "as job_id" in macro
    assert "as job_start_tstmp" in macro
    assert "as job_end_tstmp" in macro
    assert "teradata__snapshot_string_as_time(run_started_at)" in macro
    assert "run_started_at.strftime" not in macro
    assert "timestamp(6) with time zone" in macro
    assert "row_loaded_ts" not in macro


def test_audit_post_hook_stamps_only_current_unfinished_rows() -> None:
    macro = _MACRO_PATH.read_text(encoding="utf-8")

    assert "{% macro audit_columns_post_hook() %}" in macro
    assert "set job_end_tstmp = current_timestamp(6) at time zone 'GMT'" in macro
    assert "where job_id = '{{ invocation_id }}'" in macro
    assert "and job_end_tstmp is null" in macro
    assert "{% if execute %}" in macro
    assert "materialized in ['table', 'incremental'] and not catalog_name" in macro
    assert "'job_id' in columns" in macro
    assert "'job_start_tstmp' in columns" in macro
    assert "'job_end_tstmp' in columns" in macro
    assert "{% set columns = [] %}" in macro
    assert "adapter.get_columns_in_relation(this)" in macro
    assert "audit_columns_post_hook skipped for unsupported materialization" in macro
