import pytest
from dbt.tests.adapter.utils.test_timestamps import BaseCurrentTimestamps

_MODEL_CURRENT_TIMESTAMP = """
{{ config(

    materialized = 'table'

    ) }}
    select {{ current_timestamp() }} as currenttimestamp
"""

_MODEL_EXPECTED_SQL = """
    select current_timestamp(6) as currenttimestamp
"""
class TestCurrentTimestampTeradata(BaseCurrentTimestamps):
    @pytest.fixture(scope="class")
    def models(self):
        return {"get_current_timestamp.sql": _MODEL_CURRENT_TIMESTAMP}

    @pytest.fixture(scope="class")
    def expected_schema(self):
        return {
            "currenttimestamp": "TIMESTAMP WITH TIME ZONE",
        }

    @pytest.fixture(scope="class")
    def expected_sql(self):
        return _MODEL_EXPECTED_SQL