import pytest
import re

from dbt.tests.adapter.constraints.test_constraints import(
BaseModelConstraintsRuntimeEnforcement,
TestIncrementalForeignKeyConstraint
)

from fixtures import(
model_schema_yml,
my_model_sql,
my_model_with_nulls_sql,
my_model_with_quoted_column_name_sql,
model_quoted_column_schema_yml,
constrained_model_schema_yml,
my_model_wrong_order_depends_on_fk_sql,
foreign_key_model_sql,
model_fk_constraint_schema_yml,
my_model_incremental_wrong_order_depends_on_fk_sql,
create_table_macro_sql,
my_incremental_model_sql,
my_model_incremental_with_nulls_sql
)

from dbt.tests.util import (
    run_dbt,
    get_manifest,
    run_dbt_and_capture,
    write_file,
    read_file,
    relation_from_name,
)

def _normalize_whitespace(input: str) -> str:
    subbed = re.sub(r"\s+", " ", input)
    return re.sub(r"\s?([\(\),])\s?", r"\1", subbed).lower().strip()


def _find_and_replace(sql, find, replace):
    sql_tokens = sql.split()
    for idx in [n for n, x in enumerate(sql_tokens) if find in x]:
        sql_tokens[idx] = replace
    return " ".join(sql_tokens)


class TestModelConstraintsRuntimeEnforcement(BaseModelConstraintsRuntimeEnforcement):

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model.sql": my_model_wrong_order_depends_on_fk_sql,
            "foreign_key_model.sql": foreign_key_model_sql,
            "constraints_schema.yml": constrained_model_schema_yml,
        }

    @pytest.fixture(scope="class")
    def expected_sql(self):
        return """
    insert into <model_identifier> (
          id, color, date_day
        )

        select id, color, date_day
        from (


    -- depends_on: <foreign_key_model_identifier>

    select
      'blue' as color,
      1 as id,
      '2019-01-01' as date_day
        ) as model_subq
        ;
    """

    def test__model_constraints_ddl(self, project, expected_sql):
        unformatted_constraint_schema_yml = read_file("models", "constraints_schema.yml")
        write_file(
            unformatted_constraint_schema_yml.format(schema=project.test_schema),
            "models",
            "constraints_schema.yml",
        )

        results = run_dbt(["run", "-s", "+my_model"])
        # assert at least my_model was run - additional upstreams may or may not be provided to the test setup via models fixture
        assert len(results) >= 1
        generated_sql = read_file("target", "run", "test", "models", "my_model.sql")

        generated_sql_generic = _find_and_replace(generated_sql, "my_model", "<model_identifier>")
        generated_sql_generic = _find_and_replace(
            generated_sql_generic, "foreign_key_model", "<foreign_key_model_identifier>"
        )

        assert _normalize_whitespace(expected_sql) == _normalize_whitespace(generated_sql_generic)

    pass


class BaseConstraintsRuntimeDdlEnforcement:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model.sql": my_model_wrong_order_depends_on_fk_sql,
            "foreign_key_model.sql": foreign_key_model_sql,
            "constraints_schema.yml": model_fk_constraint_schema_yml,
        }

    @pytest.fixture(scope="class")
    def expected_sql(self):
        return """
    insert into <model_identifier> (
        id ,
        color ,
        date_day
    )
        select
           id,
           color,
           date_day
           from
        (
            -- depends_on: <foreign_key_model_identifier>
            select
                'blue' as color,
                1 as id,
                '2019-01-01' as date_day
        ) as model_subq
        ;
    """

    def test__constraints_ddl(self, project, expected_sql):
        unformatted_constraint_schema_yml = read_file("models", "constraints_schema.yml")
        write_file(
            unformatted_constraint_schema_yml.format(schema=project.test_schema),
            "models",
            "constraints_schema.yml",
        )

        results = run_dbt(["run", "-s", "+my_model"])
        # assert at least my_model was run - additional upstreams may or may not be provided to the test setup via models fixture
        assert len(results) >= 1

        # grab the sql and replace the model identifier to make it generic for all adapters
        # the name is not what we're testing here anyways and varies based on materialization
        generated_sql = read_file("target", "run", "test", "models", "my_model.sql")
        generated_sql_generic = _find_and_replace(generated_sql, "my_model", "<model_identifier>")
        generated_sql_generic = _find_and_replace(
            generated_sql_generic, "foreign_key_model", "<foreign_key_model_identifier>"
        )

        assert _normalize_whitespace(expected_sql) == _normalize_whitespace(generated_sql_generic)


class TestTableConstraintsRuntimeDdlEnforcement(BaseConstraintsRuntimeDdlEnforcement):
    pass


class BaseIncrementalConstraintsRuntimeDdlEnforcement(BaseConstraintsRuntimeDdlEnforcement):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model.sql": my_model_incremental_wrong_order_depends_on_fk_sql,
            "foreign_key_model.sql": foreign_key_model_sql,
            "constraints_schema.yml": model_fk_constraint_schema_yml,
        }

class TestIncrementalConstraintsRuntimeDdlEnforcement(BaseIncrementalConstraintsRuntimeDdlEnforcement):
    pass



class BaseConstraintQuotedColumn(BaseConstraintsRuntimeDdlEnforcement):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model.sql": my_model_with_quoted_column_name_sql,
            "constraints_schema.yml": model_quoted_column_schema_yml,
        }

    @pytest.fixture(scope="class")
    def expected_sql(self):
        return """
insert into <model_identifier> (
    id, "from", date_day
)
    select id, "from", date_day
    from (
        select
          'blue' as "from",
          1 as id,
          '2019-01-01' as date_day
    ) as model_subq
;
"""
class TestConstraintQuotedColumn(BaseConstraintQuotedColumn):
    pass


class TestTeradataIncrementalForeignKeyConstraint(TestIncrementalForeignKeyConstraint):
    @pytest.fixture(scope="class")
    def macros(self):
        return {
            "create_table.sql": create_table_macro_sql,
        }
    pass