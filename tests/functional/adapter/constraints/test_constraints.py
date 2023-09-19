import pytest

from dbt.tests.adapter.constraints.test_constraints import BaseIncrementalConstraintsRollback
from dbt.tests.adapter.constraints.test_constraints import BaseTableContractSqlHeader
from dbt.tests.adapter.constraints.test_constraints import BaseIncrementalContractSqlHeader
from dbt.tests.adapter.constraints.test_constraints import BaseModelConstraintsRuntimeEnforcement
from dbt.tests.adapter.constraints.test_constraints import BaseConstraintQuotedColumn

'''
******* Below test cases are enabled once Model contrants feature added in dbt-teradata ******

class TestIncrementalConstraintsRollback(BaseIncrementalConstraintsRollback):
    pass



class TestTableContractSqlHeader(BaseTableContractSqlHeader):
    pass



class TestIncrementalContractSqlHeader(BaseIncrementalContractSqlHeader):
    pass



class TestModelConstraintsRuntimeEnforcement(BaseModelConstraintsRuntimeEnforcement):
    pass



class TestConstraintQuotedColumn(BaseConstraintQuotedColumn):
    pass
'''