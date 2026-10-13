"""Functional regression test for IDE-26634.

The `valid_history` incremental strategy generates a `TD_NORMALIZE_MEET ...
RETURNS (...)` clause from `adapter.get_columns_in_relation(source)`. When a
tracked column is physically fixed-width CHAR(n), the adapter used to declare
it as VARCHAR(n) in that clause, which Teradata rejects on the second (true
incremental) run with:

    [Error 9134] Normalize: The output grouping column list does not match
    with input grouping column list

The first run never exercises this code path: with no existing target table it
takes the plain `create table as select` branch. The bug only surfaces once a
model actually runs incrementally.

This test creates a source table with a fixed-width CHAR(1) column and runs the
`valid_history` model twice, asserting both runs succeed.

A second test class covers a UNICODE fixed-width CHAR column specifically:
DBC.ColumnsV.ColumnLength is a byte length, and UNICODE characters are 2 bytes
each, so a naive read of ColumnLength as the declared size renders e.g.
"character(2)" for a CHAR(1) UNICODE column -- silently wrong in the same way
the original CHAR/VARCHAR mislabeling was, and equally capable of tripping the
TD_NORMALIZE_MEET RETURNS validation.
"""

import pytest

from dbt.contracts.results import RunStatus
from dbt.tests.util import get_connection, relation_from_name, run_dbt

schema_yml = """
version: 2
sources:
  - name: raw
    schema: "{{ target.schema }}"
    tables:
      - name: party_src
"""

party_history_sql = """
{{ config(
    materialized='incremental',
    incremental_strategy='valid_history',
    unique_key='party_id',
    valid_period='load_period',
    use_valid_to_time='no',
    resolve_conflicts='yes'
) }}

select
    party_id,
    full_name,
    marketable_flag,
    PERIOD(current_date, date '9999-12-31') as load_period
from {{ source('raw', 'party_src') }}
""".strip()


class TestValidHistoryCharColumn:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "name": "test_valid_history_char_column",
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "schema.yml": schema_yml,
            "party_history.sql": party_history_sql,
        }

    def test_valid_history_succeeds_with_fixed_width_char_column(self, project):
        # party_src is created directly with a physical CHAR(1) column
        # (marketable_flag) -- a dbt seed would infer VARCHAR and would not
        # reproduce the bug.
        project.run_sql(
            """
            create table {schema}.party_src (
                party_id varchar(10),
                full_name varchar(100),
                marketable_flag char(1),
                load_period period(date)
            )
            """
        )
        project.run_sql(
            """
            insert into {schema}.party_src
                (party_id, full_name, marketable_flag, load_period)
            values
                ('P001', 'Alice Example', 'Y',
                 period(date '2024-01-01', date '9999-12-31'))
            """
        )
        project.run_sql(
            """
            insert into {schema}.party_src
                (party_id, full_name, marketable_flag, load_period)
            values
                ('P002', 'Bob Sample', 'N',
                 period(date '2024-01-01', date '9999-12-31'))
            """
        )

        # First run: target table doesn't exist yet -> CTAS path. This always
        # succeeds, even with the bug, because it never builds the
        # TD_NORMALIZE_MEET RETURNS clause.
        results = run_dbt(["run"])
        assert len(results) == 1
        assert results[0].status == RunStatus.Success

        # Second run: target table exists -> true incremental path via
        # teradata__get_incremental_valid_history_sql. Prior to the fix, this
        # failed with Teradata [Error 9134] because marketable_flag (physically
        # CHAR(1)) was declared as "character varying(1)" in the generated
        # RETURNS clause.
        results = run_dbt(["run"])
        assert len(results) == 1
        assert results[0].status == RunStatus.Success

        # Third run for extra confidence that the fix is stable across repeats.
        results = run_dbt(["run"])
        assert len(results) == 1
        assert results[0].status == RunStatus.Success

        relation = relation_from_name(project.adapter, "party_history")
        result = project.run_sql(
            f"select count(*) as num_rows from {relation}", fetch="one"
        )
        assert result[0] == 2


class TestValidHistoryUnicodeCharColumn:
    """Regression test for the UNICODE byte-length variant of IDE-26634.

    DBC.ColumnsV.ColumnLength is a byte length: 1 byte/character for CharType
    1 (LATIN), 2 bytes/character for CharType 2 (UNICODE). A CHAR(1) column
    declared CHARACTER SET UNICODE reports ColumnLength == 2, so naively using
    ColumnLength as the declared size renders "character(2)" instead of
    "character(1)".

    This test only asserts the metadata layer (char_size / data_type), which
    is what teradata__get_columns_in_relation is responsible for getting
    right. It intentionally does NOT run the valid_history model end-to-end
    for a UNICODE column: TD_NORMALIZE_MEET's RETURNS clause has no
    CHARACTER SET clause at all (for any column, UNICODE or LATIN), so a
    UNICODE column is compared against an implicit-charset RETURNS
    declaration and still trips Error 9134 regardless of the declared size.
    That is a separate, pre-existing gap -- teradata__get_columns_in_relation
    (and the Column model it feeds) does not track CharType/character set,
    and the RETURNS clause in
    materializations/incremental/strategies.sql would need a CHARACTER SET
    clause added for every string column -- which is a broader design change
    out of scope for this fix.
    """

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "name": "test_valid_history_unicode_char_column",
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "schema.yml": schema_yml,
            "party_history.sql": party_history_sql,
        }

    def test_char_size_and_data_type_correct_for_unicode_char_column(self, project):
        # No rows are inserted: get_columns_in_relation reads DBC.ColumnsV
        # catalog metadata, not table data, so data rows have no effect on
        # the char_size/data_type assertions below. The CREATE TABLE alone
        # is enough to register the relation in the catalog.
        project.run_sql(
            """
            create table {schema}.party_src (
                party_id varchar(10),
                full_name varchar(100),
                marketable_flag char(1) character set unicode,
                load_period period(date)
            )
            """
        )

        # The reported char_size must be the declared character count (1),
        # not the byte length (2). Prior to the CharType-aware char_size
        # computation, this would have reported char_size == 2 and
        # data_type == "character(2)".
        source_relation = relation_from_name(project.adapter, "party_src")
        with get_connection(project.adapter):
            columns = {
                c.column: c
                for c in project.adapter.get_columns_in_relation(source_relation)
            }
        assert columns["marketable_flag"].char_size == 1
        assert columns["marketable_flag"].data_type == "character(1)"
