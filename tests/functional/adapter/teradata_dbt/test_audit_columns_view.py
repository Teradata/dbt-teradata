import pytest
from dbt.tests.util import run_dbt


_AUDIT_VIEW_MODEL = """
{{ config(
    materialized='view',
    post_hook="{{ audit_columns_post_hook() }}"
) }}

select
    1 as test_id,
    {{ get_audit_columns() }}
"""

_NO_AUDIT_TABLE_MODEL = """
{{ config(
    materialized='table',
    post_hook="{{ audit_columns_post_hook() }}"
) }}

select 2 as test_id
"""

_PARTIAL_AUDIT_TABLE_MODEL = """
{{ config(
    materialized='table',
    post_hook="{{ audit_columns_post_hook() }}"
) }}

select
    3 as test_id,
    cast(null as timestamp(6) with time zone) as job_end_tstmp
"""


class TestAuditColumnsView:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "audit_columns_view.sql": _AUDIT_VIEW_MODEL,
            "no_audit_table.sql": _NO_AUDIT_TABLE_MODEL,
            "partial_audit_table.sql": _PARTIAL_AUDIT_TABLE_MODEL,
        }

    def test_unsupported_or_non_audited_models_skip_post_hook(self, project):
        run_dbt(["build"])