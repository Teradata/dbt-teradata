import pytest
from dbt.tests.util import run_dbt , check_relation_types,relation_from_name

from tests.functional.adapter.teradata_dbt.teradata_fixtures import(
    test_table_in_timestamp_macro_test_csv,
    test_table_snapshot_sql,
    macros_sql
)

class Test_validate_teradata_timestamp_macro:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "name": "validate_teradata_timestamp_macro",
        }

    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "test_table_in_timestamp_macro_test.csv": test_table_in_timestamp_macro_test_csv
        }
    
    @pytest.fixture(scope="class")
    def snapshots(self):
        return {
            "test_table_snapshot.sql": test_table_snapshot_sql
        }

    @pytest.fixture(scope="class")
    def macros(self):
        return {
            "macros.sql": macros_sql
        }

    def test_validate_teradata_timestamp_macro(self,project):
        result1=run_dbt(["seed"])
        assert len(result1)==1 

        result_statuses = sorted(r.status for r in result1)
        assert result_statuses == ["success"]

        result2=run_dbt(["snapshot"])
        assert len(result2)==1

        result_statuses = sorted(r.status for r in result2)
        assert result_statuses == ["success"]