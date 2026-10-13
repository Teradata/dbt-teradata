import pytest
from dbt.tests.util import run_dbt


_AUDIT_MODEL = """
{{ config(
    materialized='table',
    post_hook="{{ audit_columns_post_hook() }}"
) }}

select
    1 as test_id,
    {{ get_audit_columns() }}
"""

_AUDIT_INCREMENTAL_MODEL = """
{{ config(
    materialized='incremental',
    incremental_strategy='append',
    post_hook="{{ audit_columns_post_hook() }}"
) }}

select
    2 as test_id,
    {{ get_audit_columns() }}
"""

_AUDIT_TEST = """
{% test audit_columns_valid(model) %}
select *
from {{ model }}
where job_id is null
   or job_start_tstmp is null
  or job_end_tstmp is null
{% endtest %}
"""

_AUDIT_SCHEMA = """
version: 2

models:
  - name: audit_columns
    columns:
      - name: job_id
        tests: [not_null]
      - name: job_start_tstmp
        tests: [not_null]
      - name: job_end_tstmp
        tests: [not_null]
    tests:
      - audit_columns_valid
  - name: audit_columns_incremental
    tests:
      - audit_columns_valid
"""


class TestAuditColumns:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "audit_columns.sql": _AUDIT_MODEL,
            "audit_columns_incremental.sql": _AUDIT_INCREMENTAL_MODEL,
            "audit_columns.yml": _AUDIT_SCHEMA,
        }

    @pytest.fixture(scope="class")
    def macros(self):
        return {"audit_columns_valid.sql": _AUDIT_TEST}

    def test_build_populates_completion_timestamp(self, project):
        run_dbt(["build"])
        run_dbt(["build"])
        relation = project.adapter.Relation.create(
            database=project.database,
            schema=project.test_schema,
            identifier="audit_columns_incremental",
        )
        row_count, invocation_count, incomplete_count = project.run_sql(
            "SELECT COUNT(*), COUNT(DISTINCT job_id), "
            "SUM(CASE WHEN job_end_tstmp IS NULL THEN 1 ELSE 0 END) "
            f"FROM {relation}",
            fetch="one",
        )
        assert row_count == 2
        assert invocation_count == 2
        assert incomplete_count == 0
