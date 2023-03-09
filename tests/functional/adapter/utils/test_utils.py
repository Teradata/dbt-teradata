import pytest
from dbt.tests.adapter.utils.test_current_timestamp import BaseCurrentTimestamp
from dbt.tests.adapter.utils.test_hash import BaseHash
from tests.functional.adapter.utils.base_utils import BaseUtils
from tests.functional.adapter.utils.fixtures_utils import( 
    seeds__data_date_trunc_csv, 
    models__test_date_trunc_sql,
    models__test_date_trunc_yml,
    seeds__data_dateadd_csv,
    models__test_dateadd_sql,
    models__test_dateadd_yml,
    seeds__data_datediff_csv,
    models__test_datediff1_sql,
    models__test_datediff2_sql,
    models__test_datediff3_sql,
    models__test_datediff_yml,
    seeds__data_replace_csv,
    models__test_replace_sql,
    models__test_replace_yml,
    seeds__data_split_part_csv,
    models__test_split_part_sql,
    models__test_split_part_yml,
    seeds__data_hash_csv,
    models__test_hash_sql,
    models__test_hash_yml,
    )


class TestCurrentTimestamp(BaseCurrentTimestamp):
    pass

class TestDateTrunc(BaseUtils):
    @pytest.fixture(scope="class")
    def seeds(self):
        return {"data_date_trunc.csv": seeds__data_date_trunc_csv}
    
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_date_trunc.yml": models__test_date_trunc_yml,
            "test_date_trunc.sql": self.interpolate_macro_namespace(
                models__test_date_trunc_sql, "date_trunc"
            ),
        }
    
    pass

class TestDateAdd(BaseUtils):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "name": "test",
            # this is only needed for BigQuery, right?
            # no harm having it here until/unless there's an adapter that doesn't support the 'timestamp' type
            "seeds": {
                "test": {
                    "data_dateadd": {
                        "+column_types": {
                            "from_time": "timestamp",
                            "resultt": "timestamp",
                        },
                    },
                },
            },
        }
    
    @pytest.fixture(scope="class")
    def seeds(self):
        return {"data_dateadd.csv": seeds__data_dateadd_csv}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_dateadd.yml": models__test_dateadd_yml,
            "test_dateadd.sql": self.interpolate_macro_namespace(
                models__test_dateadd_sql, "dateadd"
            ),
        }
    pass

class TestDateDiff(BaseUtils):
    @pytest.fixture(scope="class")
    def seeds(self):
        return {"data_datediff.csv": seeds__data_datediff_csv}
    
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_datediff.yml": models__test_datediff_yml,
            "test_datediff1.sql": self.interpolate_macro_namespace(
                models__test_datediff1_sql, "datediff"
            ),
            "test_datediff2.sql": self.interpolate_macro_namespace(
                models__test_datediff2_sql, "datediff"
            ),
            "test_datediff3.sql": self.interpolate_macro_namespace(
                models__test_datediff3_sql, "datediff"
            ),
        }
    pass


class TestHash(BaseUtils):
    @pytest.fixture(scope="class")
    def seeds(self):
        return {"data_hash.csv": seeds__data_hash_csv}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_hash.yml": models__test_hash_yml,
            "test_hash.sql": self.interpolate_macro_namespace(models__test_hash_sql, "hash"),
        }
    
    pass

class TestReplace(BaseUtils):
    @pytest.fixture(scope="class")
    def seeds(self):
        return {"data_replace.csv": seeds__data_replace_csv}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_replace.yml": models__test_replace_yml,
            "test_replace.sql": self.interpolate_macro_namespace(
                models__test_replace_sql, "replace"
            ),
        }
    pass

class TestSplitPart(BaseUtils):
    @pytest.fixture(scope="class")
    def seeds(self):
        return {"data_split_part.csv": seeds__data_split_part_csv}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_split_part.yml": models__test_split_part_yml,
            "test_split_part.sql": self.interpolate_macro_namespace(
                models__test_split_part_sql, "split_part"
            ),
        }
    pass

