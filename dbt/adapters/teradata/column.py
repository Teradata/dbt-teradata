from dataclasses import dataclass
from typing import TypeVar, Optional, Dict, Any

from dbt.adapters.base.column import Column

Self = TypeVar('Self', bound='TeradataColumn')


@dataclass
class TeradataColumn(Column):
    table_database: Optional[str] = None
    table_schema: Optional[str] = None
    table_name: Optional[str] = None
    table_type: Optional[str] = None
    column_index: Optional[int] = None
    comment: Optional[str] = None

    @property
    def quoted(self) -> str:
        return '"{}"'.format(self.column)

    @property
    def data_type(self) -> str:
        """
        Preserve the fixed-width vs. variable-width distinction that Teradata
        makes between CHARACTER and CHARACTER VARYING columns. The base
        Column.data_type property calls self.string_type(self.string_size())
        for any string column, and the base string_type() classmethod
        unconditionally renders "character varying(n)", which loses this
        distinction for CHAR(n) columns (dtype == "character"). See IDE-26634.

        string_type() itself is intentionally left as the inherited
        classmethod: dbt-core's SQLAdapter.expand_column_types() calls it as
        `self.Column.string_type(size)` (unbound, on the class), so overriding
        it as an instance method would break that schema-expansion path.
        """
        if self.is_string() and self.dtype.lower() == "character":
            return "character({})".format(self.string_size())
        return super().data_type

    def __repr__(self) -> str:
        return "<TeradataColumn {} ({})>".format(self.name, self.data_type)
