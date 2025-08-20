# import pytest
# from dbt.tests.util import write_file, run_dbt
# from dbt.tests.adapter.unit_testing.test_types import BaseUnitTestingTypes
# from dbt.tests.adapter.unit_testing.test_case_insensitivity import BaseUnitTestCaseInsensivity
# from dbt.tests.adapter.unit_testing.test_invalid_input import BaseUnitTestInvalidInput


# safe_cast_sql = """
# select
#     cast(substr(opened_at,1,10) AS date format 'yyyy-mm-dd') as opened_date from {{ ref('seed')}}
# """

# seed_csv = """
# id,name,tax_rate,opened_at
# 1,Philadelphia,0.2,2016-09-01T00:00:00
# 2,New York,0.22,2017-03-15T00:00:00
# 3,Los Angeles,0.18,2018-06-10T00:00:00
# """.lstrip()

# test_safe_cast_yml = """
# unit_tests:
#   - name: test_safe_cast
#     model: safe_cast
#     given:
#       - input: ref('seed')
#         rows:
#           - {opened_at: "2023-05-14T00:00:00"}
#     expect:
#       rows:
#           - {opened_date: 2023-05-14}
# """

# # class TestTestingTypesTeradata(BaseUnitTestingTypes):

# #     @pytest.fixture
# #     def data_types(self):
# #         # sql_value, yaml_value
# #         return [
# #             ["1", "1"],
# #             ["'1'", "1"],
# #             ["'true'", "'true'"],
# #             ["DATE '2020-01-02'", "2020-01-02"],
# #             ["TIMESTAMP '2013-11-03 00:00:00'", "2013-11-03 00:00:00"],
# #             # [
# #             #     """cast('{"bar": "baz", "balance": 7.77, "active": false}'as json)""",
# #             #     """'{"bar": "baz", "balance": 7.77, "active": false}'""",
# #             # ],
# #         ]
# #         # had to comment the last testcase related to the json data type because it was failing with below error
# #         #[Teradata Database] [Error 5771] Index not supported by UDT 'TD_JSONLATIN_LOB'. Indexes are not supported for LOB UDTs.


# # class TestUnitTestCaseInsensitivityTeradata(BaseUnitTestCaseInsensivity):
# #     pass



# # class TestUnitTestInvalidInput(BaseUnitTestInvalidInput):
# #     pass

# # class TestSafeCast():

# #     @pytest.fixture(scope="class")
# #     def project_config_update(self):
# #         return {
# #             "name": "test_safe_cast",
# #             "seeds":{
# #                 "test_safe_cast":{
# #                     "seed":{
# #                         "+column_types":{
# #                             "opened_at": "varchar(20)"
# #                         }
# #                     }
# #                 }
# #             }
# #         }

# #     @pytest.fixture(scope="class")
# #     def seeds(self):
# #         return{
# #             "seed.csv": seed_csv
# #         }
# #     @pytest.fixture(scope="class")
# #     def models(self):
# #         return {
# #             "safe_cast.sql": safe_cast_sql,
# #             "test_safe_cast.yml": test_safe_cast_yml
# #         }

# #     def test_safe_cast(self, project):
# #         result1 = run_dbt(["seed"])
# #         results = run_dbt(["run"])

# #         results = run_dbt(["test"])