from dataclasses import dataclass, field
from typing import Any, Optional, Type, TypeVar

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
            # Look up the catalog integration to get DATALAKE 3-part naming info
            from dbt.adapters.factory import get_adapter
            adapter = get_adapter(quoting)
            catalog_integration = adapter.get_catalog_integration(catalog_name)

            if catalog_integration.catalog_type == "datalake":
                datalake_name = catalog_integration.datalake_name
                otf_database = catalog_integration.otf_database
                identifier = relation_config.identifier

                # Remove keys we override to avoid "multiple values" error
                kwargs.pop("quote_policy", None)
                kwargs.pop("include_policy", None)

                # Build an OTF relation with 3-part DATALAKE naming:
                #   <datalake_name>."<otf_database>"."<table>"
                return cls.create(
                    database=datalake_name,
                    schema=otf_database,
                    identifier=identifier,
                    quote_policy={
                        "database": False,   # DATALAKE name is unquoted
                        "schema": True,      # otf_database is quoted
                        "identifier": True,  # table name is quoted
                    },
                    include_policy={
                        "database": True,    # include all 3 parts
                        "schema": True,
                        "identifier": True,
                    },
                    is_otf=True,
                    **kwargs,
                )

        # Standard Teradata relation (non-OTF)
        relation = super().create_from(quoting, relation_config, **kwargs)

        # If database and schema are both set and differ, this is a 3-part name
        # (e.g., an OTF source: datalake."otf_db"."table")
        if relation.database and relation.schema and relation.database != relation.schema:
            return relation.replace(
                include_policy=Policy(database=True, schema=True, identifier=True),
                is_otf=True,
            )

        return relation

    def render(self):
        if self.is_otf:
            # OTF relations use 3-part naming: datalake."otf_db"."table"
            # Allow both database and schema to be included
            return ".".join(
                part for _, part in self._render_iterator() if part is not None
            )
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
