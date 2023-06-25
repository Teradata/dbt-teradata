from dbt.tests.adapter.query_comment.test_query_comment import (
    BaseQueryComments,
    BaseMacroQueryComments,
    BaseMacroArgsQueryComments,
    BaseMacroInvalidQueryComments,
    BaseNullQueryComments,
    BaseEmptyQueryComments,
)


class TestQueryCommentsTeradata(BaseQueryComments):
    pass


class TestMacroQueryCommentsTeradata(BaseMacroQueryComments):
    pass


class TestMacroArgsQueryCommentsTeradata(BaseMacroArgsQueryComments):
    pass


class TestMacroInvalidQueryCommentsTeradata(BaseMacroInvalidQueryComments):
    pass


class TestNullQueryCommentsTeradata(BaseNullQueryComments):
    pass


class TestEmptyQueryCommentsTeradata(BaseEmptyQueryComments):
    pass