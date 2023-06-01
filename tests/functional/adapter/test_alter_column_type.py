import pytest
from dbt.tests.util import run_dbt, check_relations_equal, relation_from_name
from dbt.contracts.results import RunStatus

initial_source_csv = """
id,name,number
1,Michael,100
2,Kobe,200
3,Lebron,300
4,Derrick,400 """.lstrip()

added_source_csv=initial_source_csv+"""
5,Steph,500
6,Tracy,600
""".lstrip()

model_incremental_sql="""
select * from {{ source('raw', 'initial_source') }}
{% if is_incremental() %}
where id > (select max(id) from {{ this }})
{% endif %}
""".strip()

config_materialized_incremental = """
  {{ config(materialized="incremental") }}
"""

incremental_sql= model_incremental_sql+config_materialized_incremental


schema_base_yml = """
version: 2
sources:
  - name: raw
    schema: "{{ target.schema }}"
    tables:
      - name: initial_source
"""


class Base_change_column_type:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
          "name": "project_alter_column_type",
          "seeds": {"+column_types": {"name":"varchar(17)"}}

        }
    
    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "initial_source.csv": initial_source_csv
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "incremental.sql": incremental_sql, "schema.yml": schema_base_yml
        }
    
    def test_alter_column_type(self, project):
        results = run_dbt(["seed"])
        assert len(results) == 1

        relation = relation_from_name(project.adapter, "initial_source")
        result = project.run_sql(f"select count(*) as num_rows from {relation}", fetch="one")
        assert result[0] == 4

        # run command
        # the "seed_name" var changes the seed identifier in the schema file
        results = run_dbt(["run"])
        assert len(results) == 1

        project.run_sql(f"alter table {relation} add name varchar(21)")
        project.run_sql(f"insert into {relation} values (5,'Steph',500)")

        results=run_dbt(["run"])
        assert len(results) == 1

        relation=relation_from_name(project.adapter, "initial_source")
        result=project.run_sql(f"select count(*) as num_rows from {relation}", fetch="one")
        assert result[0] == 5 


class TestAlterColumnTypeTeradata(Base_change_column_type):
    pass





