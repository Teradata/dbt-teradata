from concurrent.futures import Future
from dataclasses import dataclass, asdict
from typing import Optional, List, Dict, Any, Union, Iterable
import agate

import dbt
import dbt.exceptions

from dbt.adapters.base.impl import catch_as_completed
from dbt.adapters.sql import SQLAdapter
from dbt.adapters.teradata import TeradataConnectionManager
from dbt.adapters.teradata import TeradataRelation
from dbt.adapters.teradata import TeradataColumn
from dbt.adapters.base.meta import available
from dbt.adapters.base import BaseRelation
from dbt.clients.agate_helper import DEFAULT_TYPE_TESTER
from dbt.events import AdapterLogger
logger = AdapterLogger("teradata")
from dbt.utils import executor

LIST_SCHEMAS_MACRO_NAME = 'list_schemas'
LIST_RELATIONS_MACRO_NAME = 'list_relations_without_caching'


class TeradataAdapter(SQLAdapter):
    Relation = TeradataRelation
    Column = TeradataColumn
    ConnectionManager = TeradataConnectionManager

    @classmethod
    def date_function(cls):
        return 'current_date()'

    @available
    def verify_database(self, database):
        if database.startswith('"'):
            database = database.strip('"')
        expected = self.config.credentials.database
        if database.lower() != expected.lower():
            raise dbt.exceptions.NotImplementedException(
                'Cross-db references not allowed in {} ({} vs {})'
                .format(self.type(), database, expected)
            )
        # return an empty string on success so macros can call this
        return ''

    @classmethod
    def convert_text_type(cls, agate_table: agate.Table, col_idx: int) -> str:
        return "LONG VARCHAR"

    @classmethod
    def convert_datetime_type(cls, agate_table: agate.Table, col_idx: int) -> str:
        return "TIMESTAMP(0)"

    @classmethod
    def convert_time_type(cls, agate_table: agate.Table, col_idx: int) -> str:
        return "TIME"

    @classmethod
    def convert_date_type(cls, agate_table: agate.Table, col_idx: int) -> str:
        return "DATE"

    @classmethod
    def convert_boolean_type(cls, agate_table: agate.Table, col_idx: int) -> str:
        return "BYTEINT"

    @classmethod
    def convert_number_type(
        cls, agate_table: agate.Table, col_idx: int
    ) -> str:
        decimals = agate_table.aggregate(agate.MaxPrecision(col_idx))
        return "FLOAT" if decimals else "INTEGER"

    def quote(self, identifier):
        return '"{}"'.format(identifier)

    def list_relations_without_caching(
        self, schema_relation: TeradataRelation
    ) -> List[TeradataRelation]:
        kwargs = {'schema_relation': schema_relation}
        try:
            results = self.execute_macro(
                LIST_RELATIONS_MACRO_NAME,
                kwargs=kwargs
            )
        except dbt.exceptions.RuntimeException as e:
            errmsg = getattr(e, 'msg', '')
            if f"Teradata database '{schema_relation}' not found" in errmsg:
                return []
            else:
                description = "Error while retrieving information about"
                logger.debug(f"{description} {schema_relation}: {e.msg}")
                return []

        relations = []
        for row in results:
            if len(row) != 4:
                raise dbt.exceptions.RuntimeException(
                    f'Invalid value from "teradata__list_relations_without_caching({kwargs})", '
                    f'got {len(row)} values, expected 4'
                )
            _, name, _schema, relation_type = row
            relation = self.Relation.create(
                schema=_schema,
                identifier=name,
                type=relation_type
            )
            relations.append(relation)

        return relations

    def _get_columns_for_catalog(
        self, relation: TeradataRelation
    ) -> Iterable[Dict[str, Any]]:
        columns = self.get_columns_in_relation(relation)
        for column in columns:
            # convert TeradataColumns into catalog dicts
            as_dict = asdict(column)
            as_dict['column_name'] = as_dict.pop('column', None)
            as_dict['column_type'] = as_dict.pop('dtype')
            as_dict['table_database'] = None
            yield as_dict

    def get_relation(
        self, database: str, schema: str, identifier: str
    ) -> Optional[BaseRelation]:
        if not self.Relation.include_policy.database:
            database = None

        return super().get_relation(database, schema, identifier)

    def get_catalog(self, manifest):
        schema_map = self._get_catalog_schemas(manifest)
        with executor(self.config) as tpe:
            futures: List[Future[agate.Table]] = []
            for info, schemas in schema_map.items():
                for schema in schemas:
                    futures.append(tpe.submit_connected(
                        self, schema,
                        self._get_one_catalog, info, [schema], manifest
                    ))
            catalogs, exceptions = catch_as_completed(futures)
        return catalogs, exceptions

    def _get_one_catalog(
        self, information_schema, schemas, manifest,
    ) -> agate.Table:
        if len(schemas) != 1:
            dbt.exceptions.raise_compiler_error(
                f'Expected only one schema in _get_one_catalog() for Teradata adapter, found '
                f'{schemas}'
            )

        database = information_schema.database
        schema = list(schemas)[0]

        columns: List[Dict[str, Any]] = []
        for relation in self.list_relations(database, schema):
            logger.debug("Getting table schema for relation {}", relation)
            columns.extend(self._get_columns_for_catalog(relation))
        return agate.Table.from_object(
            columns, column_types=DEFAULT_TYPE_TESTER
        )

    def check_schema_exists(self, database, schema):
        results = self.execute_macro(
            LIST_SCHEMAS_MACRO_NAME,
            kwargs={'database': database}
        )

        exists = True if schema in [row[0] for row in results] else False
        return exists

    # Methods used in adapter tests
    def update_column_sql(
        self,
        dst_name: str,
        dst_column: str,
        clause: str,
        where_clause: Optional[str] = None,
    ) -> str:
        clause = f'update {dst_name} set {dst_column} = {clause}'
        if where_clause is not None:
            clause += f' where {where_clause}'
        return clause

    def timestamp_add_sql(
        self, add_to: str, number: int = 1, interval: str = 'hour'
    ) -> str:
        # for backwards compatibility, we're compelled to set some sort of
        # default. A lot of searching has lead me to believe that the
        # '+ interval' syntax used in postgres/redshift is relatively common
        # and might even be the SQL standard's intention.
        return f"{add_to} + interval '{number}' {interval}"

    def string_add_sql(
        self, add_to: str, value: str, location='append',
    ) -> str:
        if location == 'append':
            return f"concat(cast(trim({add_to}) as varchar(63800)), '{value}')"
        elif location == 'prepend':
            return f"concat('{value}', cast(trim({add_to}) as varchar(63800))"
        else:
            raise RuntimeException(
                f'Got an unexpected location value of "{location}"'
            )

    def get_rows_different_sql(
        self,
        relation_a: TeradataRelation,
        relation_b: TeradataRelation,
        column_names: Optional[List[str]] = None,
    ) -> str:
        # This method only really exists for test reasons
        names: List[str]
        if column_names is None:
            columns = self.get_columns_in_relation(relation_a)
            names = sorted((self.quote(c.name) for c in columns))
        else:
            names = sorted((self.quote(n) for n in column_names))

        alias_a = "A"
        alias_b = "B"
        columns_csv_a = ', '.join([f"{alias_a}.{name}" for name in names])
        columns_csv_b = ', '.join([f"{alias_b}.{name}" for name in names])
        join_condition = ' AND '.join([f"{alias_a}.{name} = {alias_b}.{name}" for name in names])
        first_column = names[0]

        # Simulate an EXCEPT or MINUS operator
        COLUMNS_EQUAL_SQL = '''
        WITH
        a_except_b as (
            SELECT
                {columns_a}
            FROM {relation_a} as {alias_a}
            LEFT OUTER JOIN {relation_b} as {alias_b}
                ON {join_condition}
            WHERE {alias_b}.{first_column} is null
        ),
        b_except_a as (
            SELECT
                {columns_b}
            FROM {relation_b} as {alias_b}
            LEFT OUTER JOIN {relation_a} as {alias_a}
                ON {join_condition}
            WHERE {alias_a}.{first_column} is null
        ),
        diff_count as (
            SELECT
                1 as id,
                COUNT(*) as num_missing FROM (
                    SELECT * FROM a_except_b
                    UNION ALL
                    SELECT * FROM b_except_a
                ) as missing
        ),
        table_a as (
            SELECT COUNT(*) as num_rows FROM {relation_a}
        ),
        table_b as (
            SELECT COUNT(*) as num_rows FROM {relation_b}
        ),
        row_count_diff as (
            SELECT
                1 as id,
                table_a.num_rows - table_b.num_rows as difference
            FROM table_a, table_b
        )
        SELECT
            row_count_diff.difference as row_count_difference,
            diff_count.num_missing as num_mismatched
        FROM row_count_diff
        INNER JOIN diff_count ON row_count_diff.id = diff_count.id
        '''.strip()

        sql = COLUMNS_EQUAL_SQL.format(
            alias_a=alias_a,
            alias_b=alias_b,
            first_column=first_column,
            columns_a=columns_csv_a,
            columns_b=columns_csv_b,
            join_condition=join_condition,
            relation_a=str(relation_a),
            relation_b=str(relation_b),
        )

        return sql
