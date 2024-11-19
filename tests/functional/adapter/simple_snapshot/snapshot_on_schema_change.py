import pytest
from dbt.tests.util import run_dbt

seeds_base_csv = """
id,name,some_date
1,Easton,1981-05-20T06:46:51
2,Lillian,1978-09-03T18:10:33
3,Jeremiah,1982-03-11T03:59:51
4,Nolan,1976-05-06T20:21:35
5,Hannah,1982-06-23T05:41:26
6,Eleanor,1991-08-10T23:12:21
7,Lily,1971-03-29T14:58:02
8,Jonathan,1988-02-26T02:55:24
9,Adrian,1994-02-09T13:14:23
10,Nora,1976-03-01T16:51:39
""".lstrip()

ts_snapshot_sql = """
{% snapshot ts_snapshot %}
    {{ config(
        strategy='timestamp',
        unique_key='id',
        updated_at='some_date',
        target_database=database,
        target_schema=schema,
    )}}
    select * from {{ ref(var('seed_name', 'base')) }}
{% endsnapshot %}
""".strip()


class TestSnapshotOnSchemaChange:
    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "base.csv": seeds_base_csv,
        }

    @pytest.fixture(scope="class")
    def snapshots(self):
        return {
            "ts_snapshot.sql": ts_snapshot_sql,
        }

    def test_snapshot_on_schema_change(self, project):
        # seed command
        results = run_dbt(["seed"])

        # snapshot command
        results = run_dbt(["snapshot"])

        project.run_sql("ALTER TABLE {schema}.base ADD last_initial varchar(30)");

        results = run_dbt(["seed"])

        results = run_dbt(["snapshot"])
