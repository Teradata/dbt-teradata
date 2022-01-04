from dataclasses import dataclass

from dbt.adapters.base.relation import BaseRelation, Policy
from dbt.exceptions import RuntimeException

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
    quote_policy: TeradataQuotePolicy = TeradataQuotePolicy()
    include_policy: TeradataIncludePolicy = TeradataIncludePolicy()
    quote_character: str = '"'

    def render(self):
        if self.include_policy.database and self.include_policy.schema:
            raise RuntimeException(
                f"Got a teradata relation with schema and database set to "
                "include, but only one can be set"
            )
        return super().render()
