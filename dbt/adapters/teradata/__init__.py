from dbt.adapters.teradata.connections import TeradataConnectionManager
from dbt.adapters.teradata.connections import TeradataCredentials
from dbt.adapters.teradata.relation import TeradataRelation
from dbt.adapters.teradata.column import TeradataColumn
from dbt.adapters.teradata.impl import TeradataAdapter

from dbt.adapters.base import AdapterPlugin
from dbt.include.teradata import PACKAGE_PATH

Plugin = AdapterPlugin(
    adapter=TeradataAdapter,
    credentials=TeradataCredentials,
    include_path=PACKAGE_PATH)
