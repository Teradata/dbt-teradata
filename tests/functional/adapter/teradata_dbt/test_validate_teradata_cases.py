import pytest
from dbt.tests.util import run_dbt , check_relation_types,relation_from_name

from tests.functional.adapter.teradata_dbt.teradata_fixtures import(
    table_with_cte_sql,
    table_without_cte_sql,
    view_with_cte_sql,
    view_without_cte_sql,
    table_with_cte_comments_sql,
    schema_yaml
)

class Test_validate_teradata_cases:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "name": "validate_teradata_testcases",
            "models": {"+materialized": "table", "+materialized": "table", "+materialized": "view", "+materialized": "view", "+materialized": "table"}
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "table_with_cte.sql": table_with_cte_sql,
            "table_without_cte.sql": table_without_cte_sql,
            "view_with_cte.sql": view_with_cte_sql,
            "view_without_cte.sql": view_without_cte_sql,
            "table_with_cte_comments.sql": table_with_cte_comments_sql,
            "schema.yml": schema_yaml
        }

    def test_validate_teradata_cases(self,project):
        results = run_dbt(["run"])
        assert len(results) == 5

        result_statuses = sorted(r.status for r in results)
        assert result_statuses== ['success', 'success', 'success', 'success', 'success']

        #check_relation_types(project.adapter,{"abc":"def"})#, "table_without_cte.sql":"table"})
        relation1=relation_from_name(project.adapter,"table_with_cte")
        with_cte_result=project.run_sql(f"select count(*) as num_rows from {relation1}",fetch="one")
        assert with_cte_result[0]==1

        relation2=relation_from_name(project.adapter,"table_without_cte")
        without_cte_result=project.run_sql(f"select count(*) as num_rows from {relation2}",fetch="one")
        assert without_cte_result[0]==1

        relation3=relation_from_name(project.adapter,"view_with_cte")
        result3=project.run_sql(f"select count(*) as num_rows from {relation3}",fetch="one")
        assert result3[0]==1
        
        relation4=relation_from_name(project.adapter,"view_without_cte")
        result4=project.run_sql(f"select count(*) as num_rows from {relation4}",fetch="one")
        assert result4[0]==1

        relation5=relation_from_name(project.adapter,"table_with_cte_comments")
        result5=project.run_sql(f"select count(*) as num_rows from {relation5}",fetch="one")
        assert result5[0]==1

        catalog=run_dbt(["docs", "generate"])
        assert len(catalog.nodes)==5
        assert len(catalog.sources)==0