import pytest
from dbt.tests.util import run_dbt , check_relation_types,relation_from_name

from tests.functional.adapter.teradata_dbt.teradata_fixtures import(
    test_table_in_create_test_csv,
    create_table_with_cte_sql,
    table_no_config_sql,
    table_with_table_kind_sql,
    from_cte_table_with_table_kind_sql,
    table_with_table_option_sql,
    from_cte_table_with_table_option_sql,
    table_with_table_kind_and_table_option_sql,
    from_cte_table_with_table_kind_and_table_option_sql,
    table_with_many_table_options_sql,
    from_cte_table_with_many_table_options_sql,
    table_with_statistics_sql,
    from_cte_table_with_statistics_sql,
    table_with_options_and_statistics_sql,
    from_cte_table_with_options_and_statistics_sql,
    table_with_index_sql,
    from_cte_table_with_index_sql,
    table_with_larger_index_sql,
    from_cte_table_with_larger_index_sql,
    table_with_kind_options_index_sql,
    from_cte_table_with_kind_options_index_sql,
    table_with_kind_options_stats_index_sql,
    from_cte_table_with_kind_options_stats_index_sql
)

class Test_validate_create_table:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "name": "validate_create_table",
        }

    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "test_table_in_create_test.csv": test_table_in_create_test_csv  
        }
 
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "create_table_with_cte.sql": create_table_with_cte_sql,
            "table_no_config.sql": table_no_config_sql,
            "table_with_table_kind.sql": table_with_table_kind_sql,
            "from_cte_table_with_table_kind.sql": from_cte_table_with_table_kind_sql,
            "table_with_table_option.sql": table_with_table_option_sql,
            "from_cte_table_with_table_option.sql": from_cte_table_with_table_option_sql,
            "table_with_table_kind_and_table_option.sql": table_with_table_kind_and_table_option_sql,
            "from_cte_table_with_table_kind_and_table_option.sql": from_cte_table_with_table_kind_and_table_option_sql,
            "table_with_many_table_options.sql": table_with_many_table_options_sql,
            "from_cte_table_with_many_table_options.sql": from_cte_table_with_many_table_options_sql,
            "table_with_statistics.sql": table_with_statistics_sql,
            "from_cte_table_with_statistics.sql": from_cte_table_with_statistics_sql,
            "table_with_options_and_statistics.sql": table_with_options_and_statistics_sql,
            "from_cte_table_with_options_and_statistics.sql": from_cte_table_with_options_and_statistics_sql,
            "table_with_index.sql": table_with_index_sql,
            "from_cte_table_with_index.sql": from_cte_table_with_index_sql,
            "table_with_larger_index.sql": table_with_larger_index_sql,
            "from_cte_table_with_larger_index.sql": from_cte_table_with_larger_index_sql,
            "table_with_kind_options_index.sql": table_with_kind_options_index_sql,
            "from_cte_table_with_kind_options_index.sql": from_cte_table_with_kind_options_index_sql,
            "table_with_kind_options_stats_index.sql": table_with_kind_options_stats_index_sql,
            "from_cte_table_with_kind_options_stats_index.sql": from_cte_table_with_kind_options_stats_index_sql
        }

    def test_validate_create_table(self,project):
        # testing for dbt seed
        result1=run_dbt(["seed"])
        assert len(result1) == 1

        result_statuses = sorted(r.status for r in result1)
        assert result_statuses == ["success"]

        result2=run_dbt(["run"])
        assert len(result2)==22

        result_statuses2= sorted(r.status for r in result2)
        result_list=[]
        for i in range(22):
            result_list.append("success")
        assert result_statuses2==result_list

        
