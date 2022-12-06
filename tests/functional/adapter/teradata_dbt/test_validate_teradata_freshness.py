import pytest
from dbt.tests.util import run_dbt , check_relation_types,relation_from_name

from tests.functional.adapter.teradata_dbt.teradata_fixtures import(
    data_with_timestamp_sql,
    teradata_freshness_sources_yml
)

class Test_validate_teradata_freshness:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "name": "validate_teradata_freshness",

        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "data_with_timestamp.sql": data_with_timestamp_sql,
            "teradata_freshness_sources.yml": teradata_freshness_sources_yml
        }

    def test_validate_teradata_freshness(self,project):
        result1=run_dbt(["run"])
        assert len(result1)==1

        result_statuses = sorted(r.status for r in result1)
        assert result_statuses == ["success"]

        result2=run_dbt(["source","freshness"])
        result_statuses2 = sorted(r.status for r in result2)
        assert result_statuses2 == ["pass"]