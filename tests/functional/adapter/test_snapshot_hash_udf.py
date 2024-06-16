import pytest
import pathlib
from dbt.tests.util import run_dbt, run_dbt_and_capture, read_file

snapshot_hash_udf = """
{% snapshot snapshot_hash_udf %}
    {{ config(
        check_cols=['some_date'], unique_key='id', strategy='check',
        target_database=database, target_schema=schema,
        snapshot_hash_udf='GLOBAL_FUNCTIONS.hash_md5'
    ) }}
    select * from {{ ref(var('seed_name', 'base')) }}
{% endsnapshot %}
""".strip()

snapshot_hash_udf_default = """
{% snapshot snapshot_hash_udf_default %}
    {{ config(
        check_cols=['some_date'], unique_key='id', strategy='check',
        target_database=database, target_schema=schema
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


class Test_snapshot_hash_udf:

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "name": "test_snapshot_hash_udf"
        }

    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "base.csv": seeds_base_csv
        }

    @pytest.fixture(scope="class")
    def snapshots(self):
        return {
            "snapshot_hash_udf.sql": snapshot_hash_udf
        }

    def test_snapshot_hash_udf(self, project):
        (pathlib.Path(project.project_root) / "log_output").mkdir(parents=True, exist_ok=True)
        run_dbt(["seed"])
        run_dbt(["--log-path", "log_output","snapshot"])
        log_output = read_file("log_output", "dbt.log").replace("\n", " ").replace("\\n", " ")
        assert "GLOBAL_FUNCTIONS.hash_md5" in log_output

class Test_snapshot_hash_udf_default:

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "name": "test_snapshot_hash_udf_default"
        }

    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "base.csv": seeds_base_csv
        }

    @pytest.fixture(scope="class")
    def snapshots(self):
        return {
            "snapshot_hash_udf_default.sql": snapshot_hash_udf_default
        }

    def test_snapshot_hash_udf_default(self, project):
        (pathlib.Path(project.project_root) / "log_output").mkdir(parents=True, exist_ok=True)
        run_dbt(["seed"])
        run_dbt(["--log-path", "log_output","snapshot"])
        log_output = read_file("log_output", "dbt.log").replace("\n", " ").replace("\\n", " ")
        assert "HASHROW" in log_output
