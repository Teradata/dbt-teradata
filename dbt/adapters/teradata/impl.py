from concurrent.futures import Future
from dataclasses import dataclass, asdict
from typing import Optional, List, Dict, Any, Union, Iterable, Callable, Set, FrozenSet, Tuple, Type
import re
import agate

import dbt
import dbt_common.exceptions

from dbt.adapters.base.impl import catch_as_completed, ConstraintSupport, AdapterConfig
from dbt.adapters.base import PythonJobHelper
from dbt.adapters.contracts.connection import AdapterResponse
from dbt_common.contracts.constraints import ConstraintType
from dbt.adapters.base.relation import InformationSchema
from dbt.adapters.sql import SQLAdapter
from dbt.adapters.teradata import TeradataConnectionManager
from dbt.adapters.teradata import TeradataRelation
from dbt.adapters.teradata import TeradataColumn
from dbt.adapters.teradata.catalogs import TeradataDatalakeCatalogIntegration
from dbt.adapters.teradata.python_submissions import TeradataClientPythonJobHelper
from dbt.adapters.capability import CapabilityDict, CapabilitySupport, Support, Capability
from dbt.adapters.base.meta import available
from dbt.adapters.base import BaseRelation
from dbt_common.clients.agate_helper import DEFAULT_TYPE_TESTER, table_from_rows
from dbt_common.contracts.metadata import CatalogTable, TableMetadata, ColumnMetadata, StatsItem, StatsDict
from dbt.adapters.contracts.relation import RelationType, RelationConfig
from dbt.adapters.events.logging import AdapterLogger
logger = AdapterLogger("teradata")
from dbt_common.utils import executor

LIST_SCHEMAS_MACRO_NAME = 'list_schemas'
LIST_RELATIONS_MACRO_NAME = 'list_relations_without_caching'
GET_CATALOG_MACRO_NAME = 'get_catalog'

def _expect_row_value(key: str, row: agate.Row):
    if key not in row.keys():
        raise dbt_common.exceptions.DbtInternalError(
            f'Got a row without \'{key}\' column, columns: {row.keys()}'
        )

    return row[key]

def _catalog_filter_schemas(used_schemas: FrozenSet[Tuple[str, str]]) -> Callable[[agate.Row], bool]:
    schemas = frozenset((None, s.lower()) for d, s in used_schemas)

    def test(row: agate.Row) -> bool:
        table_database = _expect_row_value('table_database', row)
        table_schema = _expect_row_value('table_schema', row)
        if table_schema is None:
            return False
        return (table_database, table_schema.lower()) in schemas

    return test

@dataclass
class TeradataConfig(AdapterConfig):
    query_band: Optional[str] = None

class TeradataAdapter(SQLAdapter):
    Relation = TeradataRelation
    Column = TeradataColumn
    ConnectionManager = TeradataConnectionManager

    CATALOG_INTEGRATIONS = [
        TeradataDatalakeCatalogIntegration,
    ]

    CONSTRAINT_SUPPORT = {
        ConstraintType.check: ConstraintSupport.ENFORCED,
        ConstraintType.not_null: ConstraintSupport.ENFORCED,
        ConstraintType.unique: ConstraintSupport.ENFORCED,
        ConstraintType.primary_key: ConstraintSupport.ENFORCED,
        ConstraintType.foreign_key: ConstraintSupport.ENFORCED,
    }

    # adding full support for SchemaMetadataByRelations and TableLastModifiedMetadata in capability dictionary
    _capabilities: CapabilityDict = CapabilityDict(
    {
        Capability.SchemaMetadataByRelations: CapabilitySupport(support=Support.Full),
        Capability.TableLastModifiedMetadata: CapabilitySupport(support=Support.Full),
        Capability.GetCatalogForSingleRelation: CapabilitySupport(support=Support.Full),
        Capability.MicrobatchConcurrency: CapabilitySupport(support=Support.Full)
    }
    )

    @classmethod
    def date_function(cls):
        return 'current_date()'

    @property
    def python_submission_helpers(self) -> Dict[str, Type[PythonJobHelper]]:
        return {
            "client": TeradataClientPythonJobHelper,
        }

    @property
    def default_python_submission_method(self) -> str:
        return "client"

    def generate_python_submission_response(self, submission_result: Any) -> AdapterResponse:
        # Populate the full response shape (code + rows_affected) so Python-model
        # results are consistent/parseable in run_results.json, matching the
        # SQL path. (rows_affected is not tracked client-side; reported as 0.)
        return AdapterResponse(_message="OK", code="OK", rows_affected=0)

    @available
    def verify_database(self, database):
        if database.startswith('"'):
            database = database.strip('"')
        expected = self.config.credentials.schema
        if database.lower() != expected.lower():
            # Matches upstream convention: dbt-adapters raises
            # UnexpectedDbReferenceError, a dbt_common NotImplementedError
            # subclass, for exactly this guard (see dbt-postgres's
            # PostgresAdapter.verify_database).
            raise dbt_common.exceptions.NotImplementedError(
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

    @classmethod
    def quote(cls, identifier):
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
        except dbt_common.exceptions.DbtRuntimeError as e:
            errmsg = getattr(e, 'msg', '')
            if f"Teradata database '{schema_relation}' not found" in errmsg:
                return []
            else:
                logger.debug(f"Couldn’t retrieve relations from database {schema_relation}. An unexpected error was "
                             f"encountered. The Teradata error message is: {e.msg}. You may create a github issue "
                             f"with relevant details.")
                return []

        relations = []
        for row in results:
            if len(row) != 4:
                raise dbt_common.exceptions.DbtRuntimeError(
                    f"Invalid value from 'teradata__list_relations_without_caching({kwargs})', "
                    f"We expected 4 attributes but received {len(row)}. You may create a github issue with relevant "
                    f"details."
                )
            _, name, _schema, relation_type = row
            relation: BaseRelation = self.Relation.create(
                schema=_schema,
                identifier=name,
                type=relation_type
            )
            relations.append(relation)

        return relations

    def get_relation(
        self, database: str, schema: str, identifier: str
    ) -> Optional[BaseRelation]:
        # Preserve the original database before nulling it out — for OTF models,
        # this holds the datalake name (e.g. "MyOTFLake"), which we need for the
        # SAMPLE 0 fallback below.
        original_database = database
        if not self.Relation.get_default_include_policy().database:
            database = None

        # Standard lookup: queries DBC.TablesV.  Works for all regular Teradata
        # tables and views.  Returns immediately when the relation is found.
        relation = super().get_relation(database, schema, identifier)
        if relation is not None:
            return relation

        # OTF fallback: OTF (Iceberg/Delta) tables are stored in a DATALAKE
        # catalog and are NOT registered in DBC.TablesV, so the standard lookup
        # above always returns None for them.  If we have a non-None database
        # (the datalake name, preserved above), probe via SAMPLE 0.
        #
        # This makes is_incremental() evaluate correctly for OTF models: without
        # this probe it would always return False because get_relation returns None,
        # causing the model SQL to always compile without its incremental WHERE
        # filter and resulting in duplicate rows on every incremental run.
        #
        # Regular Teradata tables/views are unaffected:
        #   - Existing ones are returned by super().get_relation() above.
        #   - For non-existing ones, original_database is typically None (Teradata
        #     profiles rarely set a database), so the probe is skipped entirely.
        #   - Guard original_database != schema: for native Teradata, database is
        #     often set to the same value as schema.  For OTF, the datalake name
        #     is always distinct from the otf_database.  This prevents unnecessary
        #     SAMPLE 0 probes (and potential errors) against non-DATALAKE 3-part names.
        if original_database and schema and identifier and original_database != schema:
            if self.otf_relation_exists(original_database, schema, identifier):
                return self.Relation.create(
                    database=original_database,
                    schema=schema,
                    identifier=identifier,
                    type='table',
                    is_otf=True,
                    quote_policy={"database": True, "schema": True, "identifier": True},
                    include_policy={"database": True, "schema": True, "identifier": True},
                )

        return None

    def get_catalog(
            self,
            relation_configs: Iterable[RelationConfig],
            used_schemas: FrozenSet[Tuple[str, str]],
        ) -> Tuple[agate.Table, List[Exception]]:
        schema_map = self._get_catalog_schemas(relation_configs)
        with executor(self.config) as tpe:
            futures: List[Future[agate.Table]] = []
            for info, schemas in schema_map.items():
                for schema in schemas:
                    futures.append(tpe.submit_connected(
                        self, schema,
                        self._get_one_catalog, info, [schema], used_schemas
                    ))
            catalogs, exceptions = catch_as_completed(futures)
        return catalogs, exceptions

    def _get_one_catalog(
        self,
        information_schema: InformationSchema,
        schemas: Set[str],
        relation_config: Iterable[RelationConfig],
    ) -> agate.Table:
        if len(schemas) != 1:
            raise dbt_common.exceptions.CompilationError(
                f'Couldn’t retrieve catalog. We expected 1 input schema in _get_one_catalog() but received {schemas}. '
                f'You may create a github issue with relevant details.'
            )

        return super()._get_one_catalog(information_schema, schemas, relation_config)

    @classmethod
    def _catalog_filter_table(cls, table: agate.Table, used_schemas: FrozenSet[Tuple[str, str]]) -> agate.Table:
        table = table_from_rows(
            table.rows,
            table.column_names,
            text_only_columns=['table_schema', 'table_name'],
        )
        return table.where(_catalog_filter_schemas(used_schemas))

    def get_catalog_for_single_relation(self, relation: BaseRelation) -> Optional[CatalogTable]:
        """Get catalog information for a single relation without scanning the whole schema.

        Note: For views under the default use_qvci=False setting, the delegated
        get_columns_in_relation call creates and drops a temporary table to introspect
        view columns (DBC.ColumnsV lacks full view column metadata such as types and
        ordering, though it does store column comments). This requires CREATE TABLE
        privilege in the relation's schema (or in the schema specified by the
        ``temporary_metadata_generation_schema`` project variable).

        OTF (datalake) relations are not registered in DBC.TablesV and are not
        supported by this method; None is returned for them.
        """
        # OTF/datalake relations are not in DBC.TablesV; return None early.
        if getattr(relation, "is_otf", False):
            return None

        # Query DBC.TablesV for table-level metadata.
        # (NOT CASESPECIFIC) ensures case-insensitive matching for schema/identifier.
        # Single quotes in names are escaped by doubling them.
        # TableKind is restricted to supported types: T (table), O (NoPI table), V (view).
        schema_escaped = relation.schema.replace("'", "''")
        identifier_escaped = relation.identifier.replace("'", "''")
        # Columns are explicitly aliased because Teradata returns the underlying DBC
        # column title (e.g. DataBaseName) rather than the name used in the SELECT list.
        table_info_sql = (
            f"SELECT DatabaseName AS \"database_name\", "
            f"TableName AS \"table_name\", "
            f"TableKind AS \"table_kind\", "
            f"CommentString AS \"comment_string\" "
            f"FROM DBC.TablesV "
            f"WHERE DatabaseName = '{schema_escaped}' (NOT CASESPECIFIC) "
            f"AND TableName = '{identifier_escaped}' (NOT CASESPECIFIC) "
            f"AND TableKind IN ('T', 'V', 'O')"
        )
        _, table_result = self.execute(table_info_sql, auto_begin=True, fetch=True)

        if not table_result or len(table_result.rows) == 0:
            return None

        row = table_result.rows[0]
        table_kind = (row["table_kind"] or "").strip()
        table_comment = row["comment_string"].strip() if row["comment_string"] else None
        canonical_schema = row["database_name"].strip() if row["database_name"] else relation.schema
        canonical_name = row["table_name"].strip() if row["table_name"] else relation.identifier

        if table_kind in ('T', 'O'):
            table_type = 'table'
        elif table_kind == 'V':
            table_type = 'view'
        else:
            # Unreachable given the TableKind filter above; guard against an
            # unbound table_type if that filter is ever widened.
            return None

        table_metadata = TableMetadata(
            type=table_type,
            schema=canonical_schema,
            name=canonical_name,
            database=None,
            comment=table_comment,
            owner=None,
        )

        # The DBC.TablesV lookup above matches case-insensitively, but
        # teradata__get_columns_in_relation compares identifiers with a plain '='
        # (case-sensitive under ANSI mode). Introspect using the canonical
        # schema/identifier returned by DBC so both lookups agree.
        canonical_relation = relation.incorporate(
            path={"schema": canonical_schema, "identifier": canonical_name}
        )

        # Delegates to teradata__get_columns_in_relation which returns columns
        # ordered by ColumnID (deterministic). The duplicate DBC.TablesV lookup
        # inside the macro is acceptable overhead for a single-relation call.
        # ColumnID is an internal Teradata identifier (it does not necessarily start
        # at 1), so the catalog index is derived from the ordinal position instead.
        columns = self.get_columns_in_relation(canonical_relation)
        catalog_columns: Dict[str, ColumnMetadata] = {}
        for idx, col in enumerate(columns):
            catalog_columns[col.column] = ColumnMetadata(
                type=col.dtype,
                index=idx + 1,
                name=col.column,
                comment=col.comment.strip() if col.comment else None,
            )

        # For views with use_qvci=False, column comments from get_columns_in_relation
        # are NULL because the macro introspects a temp table copy. Backfill from
        # DBC.ColumnsV using the real view name to match the full catalog path.
        # Under use_qvci=True the comments are already populated, so the extra
        # round-trip is skipped unless at least one comment is missing.
        missing_comments = any(c.comment is None for c in catalog_columns.values())
        if table_type == 'view' and catalog_columns and missing_comments:
            comment_sql = (
                f"SELECT ColumnName AS \"column_name\", "
                f"CommentString AS \"comment_string\" "
                f"FROM DBC.ColumnsV "
                f"WHERE DatabaseName = '{schema_escaped}' (NOT CASESPECIFIC) "
                f"AND TableName = '{identifier_escaped}' (NOT CASESPECIFIC) "
                f"AND CommentString IS NOT NULL"
            )
            _, comment_result = self.execute(comment_sql, auto_begin=True, fetch=True)
            if comment_result:
                # Teradata identifiers are case-insensitive, and the casing returned by
                # DBC.ColumnsV may differ from the casing produced by
                # get_columns_in_relation(), so match on a case-folded key.
                column_key_lookup = {key.lower(): key for key in catalog_columns}
                for crow in comment_result.rows:
                    col_name = crow["column_name"].strip() if crow["column_name"] else None
                    col_comment = (
                        crow["comment_string"].strip() if crow["comment_string"] else None
                    )
                    if not col_name or not col_comment:
                        continue
                    catalog_key = column_key_lookup.get(col_name.lower())
                    if catalog_key is None:
                        continue
                    catalog_columns[catalog_key] = ColumnMetadata(
                        type=catalog_columns[catalog_key].type,
                        index=catalog_columns[catalog_key].index,
                        name=catalog_columns[catalog_key].name,
                        comment=col_comment,
                    )

        # Placeholder: has_stats is always True/include=False following the same
        # pattern as dbt-snowflake. Teradata does not expose a simple "has stats"
        # flag in DBC metadata; probing DBC.StatsV per relation would add latency
        # for minimal UI value (include=False means it is hidden by default).
        stats_dict: StatsDict = {
            "has_stats": StatsItem(
                id="has_stats",
                label="Has Stats?",
                value=True,
                include=False,
                description="Indicates whether there are statistics for this table",
            ),
        }

        return CatalogTable(
            metadata=table_metadata,
            columns=catalog_columns,
            stats=stats_dict,
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
            raise dbt_common.exceptions.DbtRuntimeError(
                f'Couldn’t modify SQL string. The location parameter was neither "append" nor "prepend". Correct the '
                f'model and retry.'
            )

    def get_rows_different_sql(
        self,
        relation_a: TeradataRelation,
        relation_b: TeradataRelation,
        column_names: Optional[List[str]] = None,
        except_operator: str = "MINUS",
    ) -> str:
        # This method only really exists for test reasons
        #
        # NOTE: an earlier implementation compared rows via a LEFT OUTER JOIN
        # keyed on NULL-safe equality across every column, then filtered on
        # "first sorted column IS NULL" to detect an unmatched row. That is
        # unsound whenever the "first" column can itself legitimately hold
        # NULL: a row that *does* match on every column (including a shared
        # NULL) would still get flagged as missing, because its NULL value
        # satisfies the "IS NULL" filter regardless of whether the join
        # actually found a partner. Using MINUS/EXCEPT (whole-row set
        # difference, which is NULL-safe by SQL definition) avoids this class
        # of false positive entirely and mirrors the base adapter's approach.
        #
        # However, MINUS/EXCEPT is a *set* operator: it collapses duplicate
        # rows before comparing, so it ignores row multiplicity. For example
        # A = [(1), (1), (2)] and B = [(1), (2), (2)] would both produce an
        # empty difference even though they don't contain the same rows the
        # same number of times. To make the comparison multiset-aware without
        # relying on EXCEPT ALL/MINUS ALL (Teradata does not support this),
        # each relation is first grouped down to (columns, row_count) pairs,
        # and it's *that* per-row multiplicity that gets compared via
        # MINUS/EXCEPT. A row whose count differs between A and B (including
        # one missing entirely, i.e. count 0) will then show up as a mismatch.
        #
        # NOTE: this adapter's default text type (convert_text_type) is
        # "LONG VARCHAR", which is Teradata's legacy bounded alias for
        # VARCHAR(64000) -- not an unbounded/LOB type -- so it groups and
        # participates in MINUS/EXCEPT like any other VARCHAR. Verified live
        # against Teradata (20.0.0.47) with both short and 5000-character
        # LONG VARCHAR values: GROUP BY and MINUS both work. This is also
        # exercised indirectly by every check_relations_equal() call in the
        # functional suite whose seeded relations contain a text column
        # (e.g. test_incremental_suite.py's on-schema-change tests, via the
        # CSV-seeded "name" column), all of which pass.
        #
        # LIMITATION: the multiplicity trick above requires GROUP BY over
        # *every* compared column, and Teradata cannot group by its
        # non-comparable types -- CLOB, BLOB, JSON, XML, ST_Geometry and
        # Period. (The LONG VARCHAR note above is specifically about the
        # bounded VARCHAR alias and does NOT generalize to CLOB.) Comparing
        # relations that contain such a column therefore fails in the
        # database with a grouping error rather than returning a row count.
        # Callers that need to compare LOB-bearing relations should pass an
        # explicit `column_names` list that omits those columns, comparing
        # them separately if required.
        #
        # This is a limitation of the database, not of the approach chosen
        # here, and it is NOT a regression introduced by moving to
        # GROUP BY + MINUS. Teradata refuses LOBs in every operation this
        # comparison could plausibly be built on -- verified live:
        #   GROUP BY <clob>          -> Error 5672 "LOBs are not allowed in
        #                               the GROUP BY clause."
        #   ... ON a.<clob> = b.<clob> -> Error 5689 "LOBs are not allowed to
        #                               be compared." (the previous
        #                               NULL-safe-join implementation)
        #   MINUS over a <clob>      -> Error 5690 "LOBs are not allowed to
        #                               be hashed."
        # So CLOB columns were never comparable by this method under any
        # implementation. Supporting them would mean comparing a surrogate
        # (e.g. a hash or a length/prefix projection) instead of the value
        # itself, which trades exact equality for collision risk and is a
        # deliberate semantic change rather than a bug fix -- it belongs in
        # its own change, not here.
        names: List[str]
        if column_names is None:
            columns = self.get_columns_in_relation(relation_a)
            names = sorted((self.quote(c.name) for c in columns))
            existing_names = {c.name.lower() for c in columns}
        else:
            names = sorted((self.quote(n) for n in column_names))
            existing_names = {n.lower() for n in column_names}
        columns_csv = ", ".join(names)

        # Pick a row-multiplicity alias that can't collide with an actual
        # source column of the same name (Teradata identifiers are
        # case-insensitive when unquoted), which would otherwise make the
        # later "SELECT ..., dbt_row_multiplicity" ambiguous.
        multiplicity_alias = "dbt_row_multiplicity"
        suffix = 0
        while multiplicity_alias.lower() in existing_names:
            suffix += 1
            multiplicity_alias = f"dbt_row_multiplicity_{suffix}"
        multiplicity_alias = self.quote(multiplicity_alias)

        COLUMNS_EQUAL_SQL = '''
        WITH grouped_a as (
            SELECT {columns}, COUNT(*) as {multiplicity_alias}
            FROM {relation_a}
            GROUP BY {columns}
        ),
        grouped_b as (
            SELECT {columns}, COUNT(*) as {multiplicity_alias}
            FROM {relation_b}
            GROUP BY {columns}
        ),
        diff_count as (
            SELECT
                1 as id,
                COUNT(*) as num_missing FROM (
                    (SELECT {columns}, {multiplicity_alias} FROM grouped_a {except_op}
                     SELECT {columns}, {multiplicity_alias} FROM grouped_b)
                     UNION ALL
                    (SELECT {columns}, {multiplicity_alias} FROM grouped_b {except_op}
                     SELECT {columns}, {multiplicity_alias} FROM grouped_a)
                ) as a
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
            columns=columns_csv,
            multiplicity_alias=multiplicity_alias,
            except_op=except_operator,
            relation_a=str(relation_a),
            relation_b=str(relation_b),
        )

        return sql

    
    @available
    def standardize_grants_dict(self, grants_table: agate.Table) -> dict:
        """Override to strip extra whitespace from Teradata result values and to
        support role-aware grants (teradata_grants_extended=true).

        When the extended mode is active, get_show_grant_sql returns a
        'grantee_type' column ('user' or 'role').  Every grantee is then
        qualified with its type ('role:<name>' / 'user:<name>'), matching the
        convention dbt-redshift uses for its own extended grants.  The
        teradata__apply_grants macro normalizes the grants config the same way
        (bare names become 'user:<name>'), so both sides of dbt's diff use the
        identical representation and grants stay idempotent across runs.

        Without the column the behavior is identical to the base implementation:
        bare user names, fully backward compatible.
        """
        grants_dict: Dict[str, List[str]] = {}
        has_grantee_type = "grantee_type" in grants_table.column_names
        for row in grants_table:
            grantee = row["grantee"].strip()
            privilege = row["privilege_type"].strip()
            if has_grantee_type:
                grantee_type = row["grantee_type"].strip().lower()
                grantee = f"role:{grantee}" if grantee_type == "role" else f"user:{grantee}"
            if privilege in grants_dict:
                grants_dict[privilege].append(grantee)
            else:
                grants_dict[privilege] = [grantee]
        return grants_dict

    def valid_incremental_strategies(self):
        """The set of standard builtin strategies which this adapter supports out-of-the-box.
        Not used to validate custom strategies defined by end users.
        """
        return ["delete+insert","append","merge", "valid_history", "microbatch"]

    @available
    def otf_relation_exists(self, datalake_name: str, otf_database: str, identifier: str) -> bool:
        """Check whether an OTF (DATALAKE) table exists on the server.

        OTF tables are not registered in DBC.TablesV under the dbt target
        schema, so load_relation() / list_relations_without_caching() cannot
        be used.  Instead, probe the 3-part DATALAKE name with SAMPLE 0:

            SELECT * FROM "<datalake>"."<otf_db>"."<identifier>" SAMPLE 0

        This succeeds (returning 0 rows) when the table exists, and raises a
        "table does not exist" error when it does not. The exact error code
        depends on the Teradata / OTF engine version:
          - Error 7825 = ICEBERG_EXPORT "Table does not exist" (external catalog)
          - Error 6321 = "OTF Error: Table does not exist" (newer OTF engines,
                         e.g. 20.0.0.61)
        Both indicate a missing OTF table and are treated as "does not exist".

        Returns True if the table exists, False otherwise.
        """
        otf_name = self.Relation.create(
            database=datalake_name,
            schema=otf_database,
            identifier=identifier,
            is_otf=True,
        ).render()
        try:
            self.connections.execute(
                f"SELECT * FROM {otf_name} SAMPLE 0",
                auto_begin=False,
                fetch=False,
            )
            return True
        except dbt_common.exceptions.DbtDatabaseError as ex:
            # "Table does not exist" for an OTF table surfaces under different
            # error codes across Teradata/OTF versions (7825 on older external
            # catalog paths, 6321 on newer OTF engines). Swallow only these
            # specific "not found" codes; re-raise auth/network/syntax failures.
            msg = str(ex)
            if "[Error 7825]" in msg or "[Error 6321]" in msg:
                return False
            raise

    @available
    def get_otf_columns_in_relation(self, datalake_name: str, otf_database: str, identifier: str) -> List[str]:
        """Return the column NAMES of an OTF (DATALAKE) table.

        OTF tables are not registered in DBC.ColumnsV, and HELP COLUMN raises
        Error 7825 for them, so the normal get_columns_in_relation() path cannot
        read an OTF table's schema.  Instead, read the result-set metadata from a
        zero-row probe:

            SELECT * FROM "<datalake>"."<otf_db>"."<table>" SAMPLE 0

        The agate result carries the column names even with no rows.  Column
        *types* are not reliably recoverable this way, so only names are
        returned; callers that need a new column's DDL type source it from the
        (regular Teradata) staging table instead.  Used by on_schema_change
        handling to diff the OTF target against the incoming staging schema.
        """
        otf_name = self.Relation.create(
            database=datalake_name,
            schema=otf_database,
            identifier=identifier,
            is_otf=True,
        ).render()
        _, table = self.connections.execute(
            f"SELECT * FROM {otf_name} SAMPLE 0",
            auto_begin=False,
            fetch=True,
        )
        return list(table.column_names)

    @available
    def get_otf_column_types(self, datalake_name: str, otf_database: str, identifier: str) -> List[Dict[str, str]]:
        """Return the columns of an OTF table as ``[{'name', 'otf_type'}]``.

        Uses ``HELP TABLE "<datalake>"."<otf_db>"."<table>"``, which (unlike
        ``HELP COLUMN`` and ``DBC.ColumnsV``) works for OTF tables and exposes an
        ``OTF Type`` column carrying the canonical Iceberg/Delta type --
        ``int``, ``long``, ``string`` (no length), ``decimal(p, s)``, ``double``,
        ``date``, ``timestamp``, etc.

        Comparing on the OTF type (rather than the Teradata DDL type) is what
        makes ``sync_all_columns`` correct: OTF does not preserve VARCHAR length
        or SMALLINT vs INTEGER, so those never appear as spurious type changes.
        Names and types are lower-cased and whitespace-normalised.
        """
        otf_name = self.Relation.create(
            database=datalake_name,
            schema=otf_database,
            identifier=identifier,
            is_otf=True,
        ).render()
        _, table = self.connections.execute(
            f"HELP TABLE {otf_name}",
            auto_begin=False,
            fetch=True,
        )

        def _norm(value: Any) -> str:
            return re.sub(r"\s+", " ", str(value).strip()).lower() if value is not None else ""

        columns: List[Dict[str, str]] = []
        for row in table.rows:
            name = _norm(row["Column Name"])
            otf_type = _norm(row["OTF Type"])
            if name:
                columns.append({"name": name, "otf_type": otf_type})
        return columns

    @available
    def teradata_type_to_otf_type(self, data_type: Optional[str]) -> str:
        """Map a Teradata column DDL type (e.g. from a staging table) to the
        canonical OTF/Iceberg type reported by ``HELP TABLE`` ``OTF Type``.

        Lets ``sync_all_columns`` compare the incoming (staging) schema against
        the OTF target on the same vocabulary, so that differences OTF does not
        represent (VARCHAR length, SMALLINT vs INTEGER) are not flagged as type
        changes. Best-effort: unknown types fall back to their normalised string.
        """
        t = re.sub(r"\s+", " ", (data_type or "").strip()).lower()
        m = re.match(r"^(?:decimal|numeric)\s*\(\s*(\d+)\s*,\s*(\d+)\s*\)", t)
        if m:
            return f"decimal({m.group(1)}, {m.group(2)})"
        if t.startswith(("decimal", "numeric")):
            return "decimal(38, 0)"
        if t.startswith(("varchar", "char", "long varchar", "clob", "character", "vargraphic", "graphic")):
            return "string"
        if t.startswith("bigint"):
            return "long"
        if t.startswith(("integer", "int", "smallint", "byteint")):
            return "int"
        if t.startswith(("float", "real", "double", "number")):
            return "double"
        if t.startswith("timestamp"):
            return "timestamp"
        if t.startswith("date"):
            return "date"
        if t.startswith("time"):
            return "time"
        if t.startswith(("byte", "varbyte", "blob", "binary")):
            return "binary"
        return t

    @available
    def otf_type_promotion_allowed(self, old_otf_type: Optional[str], new_otf_type: Optional[str]) -> bool:
        """Whether changing a column from ``old_otf_type`` to ``new_otf_type`` is
        a type promotion OTF/Iceberg permits via ``ALTER ... MODIFY``.

        Verified against the engine: ``int -> long`` and decimal *precision*
        widening (same scale) are allowed; scale changes, narrowing, and string
        changes are not. Equal types are trivially allowed (no-op).
        """
        old = (old_otf_type or "").strip().lower()
        new = (new_otf_type or "").strip().lower()
        if old == new:
            return True
        if old == "int" and new == "long":
            return True
        mo = re.match(r"^decimal\(\s*(\d+)\s*,\s*(\d+)\s*\)$", old)
        mn = re.match(r"^decimal\(\s*(\d+)\s*,\s*(\d+)\s*\)$", new)
        if mo and mn and mo.group(2) == mn.group(2) and int(mn.group(1)) >= int(mo.group(1)):
            return True
        return False

