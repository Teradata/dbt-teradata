import pytest
from dbt.tests.util import run_dbt, run_dbt_and_capture

view_query_band_sql = """
sel 1 as fun
"""

table_query_band_sql = """
{{ config(materialized="table") }}
sel 2 as fun
"""

incremental_query_band_sql = """
{{ config(materialized="incremental") }}
sel 3 as fun
"""

snapshot_query_band_sql = """
{% snapshot snapshot_query_band %}
    {{ config(
        check_cols=['some_date'], unique_key='id', strategy='check',
        target_database=database, target_schema=schema,
        query_band='sql={model};'
    ) }}
    select * from {{ ref(var('seed_name', 'base')) }}
{% endsnapshot %}
""".strip()

seeds_base_csv = """
id,name,some_date
1,Easton,1981-05-20T06:46:51
2,Lillian,1978-09-03T18:10:33
3,Jeremiah,1982-03-11T03:59:51
4,Nolan,1976-05-06T20:21:35
""".lstrip()


# fetch_query_band = """
# {% macro fetch_query_band() %}
#     {% set output = run_query("sel GetQueryBand();") %}
#     {{ output }}
# {% endmacro %}
# """


class Test_query_band:

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "name": "test_query_band",
            "models": {
                "test_query_band": {
                    "query_band": "sql={model};"
                }
            }
        }

    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "base.csv": seeds_base_csv
        }

    # @pytest.fixture(scope="class")
    # def macros(self):
    #     return {"fetch_query_band.sql": fetch_query_band}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "view_query_band.sql": view_query_band_sql,
            "table_query_band.sql": table_query_band_sql,
            "incremental_query_band.sql": incremental_query_band_sql
        }

    @pytest.fixture(scope="class")
    def snapshots(self):
        return {
            "snapshot_query_band.sql": snapshot_query_band_sql
        }

    def test_query_band(self, project):
        results = run_dbt(["run"])
        assert len(results) == 3

        run_dbt(["seed"])
        run_dbt(["snapshot"])
        # _, out = run_dbt_and_capture(["run-operation", "fetch_query_band"], expect_pass=False)
        # assert "view_query_band" in out

        project.run_sql(f"flush query logging with all")

        result1 = project.run_sql(f"sel queryband from dbc.dbqlogtbl where queryband like '%view_query_band%'",
                                  fetch="one")
        assert "view_query_band" in result1[0]

        result2 = project.run_sql(f"sel queryband from dbc.dbqlogtbl where queryband like '%table_query_band%'",
                                  fetch="one")
        assert "table_query_band" in result2[0]

        result3 = project.run_sql(f"sel queryband from dbc.dbqlogtbl where queryband like '%incremental_query_band%'",
                                  fetch="one")
        assert "incremental_query_band" in result3[0]

        result4 = project.run_sql(f"sel queryband from dbc.dbqlogtbl where queryband like '%snapshot_query_band%'",
                                  fetch="one")
        assert "snapshot_query_band" in result4[0]

        result5 = project.run_sql(f"sel queryband from dbc.dbqlogtbl where queryband like "
                                  f"'%org=teradata-internal-telem;appname=dbt%'", fetch="one")
        assert "org=teradata-internal-telem;appname=dbt;" in result5[0]
