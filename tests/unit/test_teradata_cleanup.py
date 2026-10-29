from unittest.mock import MagicMock, call

import pytest

from tests.teradata_cleanup import cleanup_test_databases


def test_cleanup_deletes_contents_before_dropping_databases():
    connection = MagicMock()
    cursor = connection.cursor.return_value.__enter__.return_value
    cursor.fetchall.return_value = [("dbt_test_123",), ('dbt_test_a"b',)]

    cleanup_test_databases(connection)

    query = cursor.execute.call_args_list[0].args[0]
    assert "DBKind = 'D'" in query
    assert "SUBSTR(DatabaseName, 1, 9) = 'dbt_test_'" in query
    assert cursor.execute.call_args_list[1:] == [
        call('DELETE DATABASE "dbt_test_123" ALL;'),
        call('DROP DATABASE "dbt_test_123";'),
        call('DELETE DATABASE "dbt_test_a""b" ALL;'),
        call('DROP DATABASE "dbt_test_a""b";'),
    ]


def test_cleanup_without_matching_databases():
    connection = MagicMock()
    cursor = connection.cursor.return_value.__enter__.return_value
    cursor.fetchall.return_value = []

    cleanup_test_databases(connection)

    assert cursor.execute.call_count == 1


@pytest.mark.parametrize("name", ["DBC", "dbtXtest_123", "dbt_test"])
def test_cleanup_refuses_non_test_database(name):
    connection = MagicMock()
    cursor = connection.cursor.return_value.__enter__.return_value
    cursor.fetchall.return_value = [(name,)]

    with pytest.raises(ValueError, match="Refusing to clean non-test database"):
        cleanup_test_databases(connection)

    assert cursor.execute.call_count == 1


def test_cleanup_propagates_database_errors_without_dropping():
    connection = MagicMock()
    cursor = connection.cursor.return_value.__enter__.return_value
    cursor.fetchall.return_value = [("dbt_test_123",)]
    cursor.execute.side_effect = [None, RuntimeError("Database is in use")]

    with pytest.raises(RuntimeError, match="Database is in use"):
        cleanup_test_databases(connection)

    assert cursor.execute.call_count == 2
