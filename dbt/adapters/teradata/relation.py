from dataclasses import dataclass, field
from typing import Any, Type, TypeVar

from dbt.adapters.base.relation import BaseRelation, Policy
from dbt.adapters.contracts.relation import HasQuoting, RelationConfig
from dbt_common.exceptions import DbtRuntimeError

Self = TypeVar("Self", bound="TeradataRelation")


@dataclass
class TeradataQuotePolicy(Policy):
    database: bool = False
    schema: bool = True
    identifier: bool = True


@dataclass
class TeradataIncludePolicy(Policy):
    database: bool = False
    schema: bool = True
    identifier: bool = True


@dataclass(frozen=True, eq=False, repr=False)
class TeradataRelation(BaseRelation):
    quote_policy: Policy = field(default_factory=lambda: TeradataQuotePolicy())
    include_policy: Policy = field(default_factory=lambda: TeradataIncludePolicy())
    quote_character: str = '"'
    is_otf: bool = False

    @classmethod
    def create_from(
        cls: Type[Self],
        quoting: HasQuoting,
        relation_config: RelationConfig,
        **kwargs: Any,
    ) -> Self:
        # Check if the referenced model has catalog_name (i.e. it's an OTF table)
        catalog_name = None
        if hasattr(relation_config, "config") and relation_config.config:
            catalog_name = (
                relation_config.config.get("catalog_name")
                if hasattr(relation_config.config, "get")
                else getattr(relation_config.config, "catalog_name", None)
            )

        if catalog_name:
            # Lazy import to avoid a circular dependency: dbt.adapters.factory
            # imports adapter classes during registration, which would re-enter
            # this module if imported at module load time.
            from dbt.adapters.factory import get_adapter
            adapter = get_adapter(quoting)
            catalog_integration = adapter.get_catalog_integration(catalog_name)

            if catalog_integration.catalog_type == "datalake":
                # Build a clean kwargs dict — quote_policy/include_policy are
                # set explicitly below, so any caller-supplied versions must
                # be discarded to avoid "multiple values for keyword argument".
                forwarded_kwargs = {
                    k: v for k, v in kwargs.items()
                    if k not in ("quote_policy", "include_policy")
                }

                # Build an OTF relation with 3-part DATALAKE naming:
                #   "<datalake_name>"."<otf_database>"."<table>"
                return cls.create(
                    database=catalog_integration.datalake_name,
                    schema=catalog_integration.otf_database,
                    identifier=relation_config.identifier,
                    quote_policy={
                        "database": True,    # DATALAKE name is quoted
                        "schema": True,      # otf_database is quoted
                        "identifier": True,  # table name is quoted
                    },
                    include_policy={
                        "database": True,    # include all 3 parts
                        "schema": True,
                        "identifier": True,
                    },
                    is_otf=True,
                    **forwarded_kwargs,
                )

        # Standard Teradata relation (non-OTF)
        relation = super().create_from(quoting, relation_config, **kwargs)

        # In Teradata, normal objects use 2-part naming (database.object, where
        # dbt's `database` and `schema` collapse to the same Teradata database).
        # Only OTF objects use 3-part naming (catalog.schema.object). Therefore,
        # if a relation arrives with both `database` and `schema` set to
        # *different* values, it can only be an OTF reference (typically
        # declared in sources.yml without a `catalog_name` model config).
        #
        # This heuristic is safe because TeradataCredentials.__post_init__
        # (connections.py) raises a DbtRuntimeError when the profile sets
        # `database` to a value different from `schema`. Native cross-database
        # references with database != schema cannot reach this point.
        if relation.database and relation.schema and relation.database != relation.schema:
            return relation.replace(
                include_policy=Policy(database=True, schema=True, identifier=True),
                is_otf=True,
            )

        return relation

    def render(self):
        if self.is_otf:
            # OTF relations use 3-part naming: "<datalake>"."<otf_db>"."<table>".
            # Quote and escape all three parts explicitly without relying on private APIs.
            # NOTE: this quoting (wrap in quote_character, double any embedded
            # quote) is mirrored by the Jinja macro teradata__quote_otf_part in
            # macros/materializations/otf/create_otf_table_as.sql. Keep the two in
            # sync so OTF DDL/DML and relation rendering produce identical names.
            def _quote(part: str) -> str:
                qc = self.quote_character or '"'
                return qc + part.replace(qc, qc * 2) + qc

            if self.database is None or self.schema is None:
                raise DbtRuntimeError(
                    f"OTF relation is missing required part(s): "
                    f"database={self.database!r}, schema={self.schema!r}, "
                    f"identifier={self.identifier!r}"
                )
            if self.identifier is None:
                # Schema-only OTF relation (e.g. cache warming via .without_identifier()).
                return f"{_quote(self.database)}.{_quote(self.schema)}"
            return f"{_quote(self.database)}.{_quote(self.schema)}.{_quote(self.identifier)}"
        if self.include_policy.database and self.include_policy.schema:
            raise DbtRuntimeError(
                f"Got a teradata relation with schema and database set to "
                "include, but only one can be set"
            )
        return super().render()

    ''' overriding render_limited() method because super method uses LIMIT clause which is not supported in Teradata
        This method is used when --empty flag in dbt run command is used for dry run of models '''
    def render_limited(self) -> str:
        rendered = self.render()
        if self.limit is None:
            return rendered
        elif self.limit == 0:
            return f"(select * from {rendered} sample 0) _dbt_limit_subq"
        else:
            return f"(select * from {rendered} sample {self.limit}) _dbt_limit_subq"
