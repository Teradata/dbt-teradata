from unittest import mock

import pytest
from dbt.tests.util import run_dbt

sample_seed = """sample_num,sample_bool
1,true
2,false
3,true
"""

second_seed = """sample_num,sample_bool
4,true
5,false
6,true
"""

sample_config = """
sources:
  - name: my_source_schema
    schema: "{{ target.schema }}"
    tables:
      - name: sample_source
      - name: second_source
      - name: non_existent_source
      - name: source_from_seed
"""

class TestBaseGenerate:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model.sql": "select 1 as fun",
            "alt_model.sql": "select 1 as notfun",
            "sample_config.yml": sample_config,
        }

    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "sample_seed.csv": sample_seed,
            "second_seed.csv": sample_seed,
        }

class TestGenerateSelectOverMaxSchemaMetadataRelations(TestBaseGenerate):
    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "sample_seed.csv": sample_seed,
            "second_seed.csv": sample_seed,
            "source_from_seed.csv": sample_seed,
        }

    def test_select_source(self, project):
        run_dbt(["build"])

        project.run_sql("create table {}.sample_source (id int)".format(project.test_schema))
        project.run_sql("create table {}.second_source (id int)".format(project.test_schema))

        with mock.patch.object(type(project.adapter), "MAX_SCHEMA_METADATA_RELATIONS", 1):
            # more relations than MAX_SCHEMA_METADATA_RELATIONS -> all sources and nodes correctly returned
            catalog = run_dbt(["docs", "generate"])
            assert len(catalog.sources) == 3
            assert len(catalog.nodes) == 5

            # full source selection respected
            catalog = run_dbt(["docs", "generate", "--select", "source:*"])
            assert len(catalog.sources) == 3
            assert len(catalog.nodes) == 0

            # full node selection respected
            catalog = run_dbt(["docs", "generate", "--exclude", "source:*"])
            assert len(catalog.sources) == 0
            assert len(catalog.nodes) == 5

            # granular source selection respected (> MAX_SCHEMA_METADATA_RELATIONS selected sources)
            catalog = run_dbt(
                [
                    "docs",
                    "generate",
                    "--select",
                    "source:test.my_source_schema.sample_source",
                    "source:test.my_source_schema.second_source",
                ]
            )
            assert len(catalog.sources) == 2
            assert len(catalog.nodes) == 0

            # granular node selection respected (> MAX_SCHEMA_METADATA_RELATIONS selected nodes)
            catalog = run_dbt(["docs", "generate", "--select", "my_model", "alt_model"])
            assert len(catalog.sources) == 0
            assert len(catalog.nodes) == 2

