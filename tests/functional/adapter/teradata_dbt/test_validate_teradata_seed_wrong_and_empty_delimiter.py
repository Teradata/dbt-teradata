import pytest
from dbt.tests.adapter.simple_seed.test_seed import SeedConfigBase

from tests.functional.adapter.teradata_dbt.teradata_fixtures import(
    seeds__expected_create_sql,
    seeds__expected_sql,
)

from dbt.tests.adapter.simple_seed.seeds import (
    seeds__pipe_separated_csv,
)

from dbt.tests.adapter.simple_seed.fixtures import (
    models__downstream_from_seed_pipe_separated,
)

from dbt.tests.util import (
    run_dbt,
    check_relations_equal,
    check_table_does_exist,
    check_table_does_not_exist,
)

class SeedUniqueDelimiterTestBase(SeedConfigBase):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "seeds": {"quote_columns": False, "delimiter": "|"},
        }

    @pytest.fixture(scope="class", autouse=True)
    def setUp(self, project):
        """Create table for ensuring seeds and models used in tests build correctly"""
        project.run_sql(seeds__expected_create_sql)
        project.run_sql(seeds__expected_sql)

    @pytest.fixture(scope="class")
    def seeds(self, test_data_dir):
        return {"seed_pipe_separated.csv": seeds__pipe_separated_csv}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "models__downstream_from_seed_pipe_separated.sql": models__downstream_from_seed_pipe_separated,
        }

    def _build_relations_for_test(self, project):
        """The testing environment needs seeds and models to interact with"""
        seed_result = run_dbt(["seed"])
        assert len(seed_result) == 1
        check_relations_equal(project.adapter, ["seed_expected", "seed_pipe_separated"])

        run_result = run_dbt()
        assert len(run_result) == 1
        check_relations_equal(
            project.adapter, ["models__downstream_from_seed_pipe_separated", "seed_expected"]
        )

    def _check_relation_end_state(self, run_result, project, exists: bool):
        assert len(run_result) == 1
        check_relations_equal(project.adapter, ["seed_pipe_separated", "seed_expected"])
        if exists:
            check_table_does_exist(project.adapter, "models__downstream_from_seed_pipe_separated")
        else:
            check_table_does_not_exist(
                project.adapter, "models__downstream_from_seed_pipe_separated"
            )


class TestSeedWithWrongDelimiter(SeedUniqueDelimiterTestBase):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "seeds": {"quote_columns": False, "delimiter": ";"},
        }

    def test_seed_with_wrong_delimiter(self, project):
        """Testing failure of running dbt seed with a wrongly configured delimiter"""
        seed_result = run_dbt(["seed"], expect_pass=False)
        assert "syntax error" in seed_result.results[0].message.lower()


class TestSeedWithEmptyDelimiter(SeedUniqueDelimiterTestBase):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "seeds": {"quote_columns": False, "delimiter": ""},
        }

    def test_seed_with_empty_delimiter(self, project):
        """Testing failure of running dbt seed with an empty configured delimiter value"""
        seed_result = run_dbt(["seed"], expect_pass=False)
        assert "compilation error" in seed_result.results[0].message.lower()