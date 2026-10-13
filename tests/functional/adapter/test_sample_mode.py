import pytest

from dbt.tests.adapter.sample_mode.test_sample_mode import BaseSampleModeTest


class TestSampleModeTeradata(BaseSampleModeTest):
    @pytest.fixture(scope="class")
    def input_model_sql(self) -> str:
        # The upstream fixture appends a Postgres-style "-0" (UTC) offset to
        # each TIMESTAMP literal. Teradata's TIMESTAMP literal grammar
        # doesn't accept a bare timezone offset suffix (Error 3706: Invalid
        # TimeStamp Literal) unless the column is TIMESTAMP WITH TIME ZONE,
        # so drop the offset and use a plain TIMESTAMP literal instead.
        # Teradata also requires every branch of a UNION/UNION ALL to
        # reference a table (Error 3888: "A SELECT for a UNION,INTERSECT
        # or MINUS must reference a table"), so wrap each constant row in
        # a derived table instead of selecting bare literals.
        return """
{{ config(materialized='table', event_time='event_time') }}
select * from (select 1 as id, TIMESTAMP '2025-01-01 01:25:00' as event_time) t1
UNION ALL
select * from (select 2 as id, TIMESTAMP '2025-01-02 13:47:00' as event_time) t2
UNION ALL
select * from (select 3 as id, TIMESTAMP '2025-01-03 01:32:00' as event_time) t3
""".strip()
