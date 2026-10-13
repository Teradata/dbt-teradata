import pytest

from dbt.tests.adapter.constraints.test_constraints import (
    BaseContractSqlHeader,
    BaseIncrementalContractSqlHeader,
    BaseTableContractSqlHeader,
)
# BaseModelConstraintsRuntimeEnforcement and BaseIncrementalForeignKeyConstraint
# are already covered in tests/functional/adapter/constraints/test_constraints.py

# All of BaseConstraintsColumnsEqual (+ Table/View/Incremental variants),
# BaseConstraintsRuntimeDdlEnforcement (+ Incremental variant),
# BaseConstraintsRollback (+ Incremental variant), and BaseConstraintQuotedColumn
# were previously imported here from dbt.tests.adapter.constraints.test_constraints
# and skipped wholesale because their fixtures use PostgreSQL-only types (TEXT,
# bool, timestamptz, ARRAY, JSON). Those classes have been removed from this file:
# tests/functional/adapter/constraints/test_constraints.py,
# test_constraints_columns_equal.py, and test_constraints_rollback.py already
# provide fully Teradata-native reimplementations of the same scenarios (using
# VARCHAR/INTEGER/TIMESTAMP types and Teradata-valid SQL) and all pass, so
# keeping duplicate always-skipped classes here added no coverage.
#
# BaseContractSqlHeader (+ Table/Incremental variants) remain genuinely
# uncovered and are kept skipped below: their fixtures rely on a
# `set_sql_header` statement (`set session time zone 'Asia/Kolkata'`) that
# precedes the model's CREATE TABLE/VIEW AS SELECT in the same multi-statement
# request. Verified live against Teradata that any `SET ...` session statement
# in that position raises "[Error 3932] Only an ET or null statement is legal
# after a DDL Statement" — Teradata's driver treats such a SET as a DDL
# statement that must be the sole statement in its transaction, so no
# Teradata-valid substitute SQL header exists for this fixture shape.
_SKIP_REASON = (
    "set_sql_header prepends a session-level SET statement ahead of the model's "
    "CREATE TABLE/VIEW AS SELECT in one multi-statement request. Teradata rejects "
    "this ([Error 3932] Only an ET or null statement is legal after a DDL "
    "Statement) for any SET statement in that position, so this fixture has no "
    "Teradata-valid form."
)


@pytest.mark.skip(reason=_SKIP_REASON)
class TestContractSqlHeaderTeradata(BaseContractSqlHeader):
    pass


@pytest.mark.skip(reason=_SKIP_REASON)
class TestTableContractSqlHeaderTeradata(BaseTableContractSqlHeader):
    pass


@pytest.mark.skip(reason=_SKIP_REASON)
class TestIncrementalContractSqlHeaderTeradata(BaseIncrementalContractSqlHeader):
    pass
