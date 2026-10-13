import pytest

from dbt.tests.adapter.utils.test_any_value import BaseAnyValue
from dbt.tests.adapter.utils.test_array_append import BaseArrayAppend
from dbt.tests.adapter.utils.test_array_concat import BaseArrayConcat
from dbt.tests.adapter.utils.test_array_construct import BaseArrayConstruct
from dbt.tests.adapter.utils.test_bool_or import BaseBoolOr
from dbt.tests.adapter.utils.test_cast import BaseCast
from dbt.tests.adapter.utils.test_cast_bool_to_text import BaseCastBoolToText
from dbt.tests.adapter.utils.test_concat import BaseConcat
from dbt.tests.adapter.utils.test_current_timestamp import (
    BaseCurrentTimestampAware,
    BaseCurrentTimestampNaive,
)
from dbt.tests.adapter.utils import fixture_date
from dbt.tests.adapter.utils import fixture_escape_single_quotes
from dbt.tests.adapter.utils import fixture_position
from dbt.tests.adapter.utils.test_date import BaseDate
from dbt.tests.adapter.utils.test_escape_single_quotes import (
    BaseEscapeSingleQuotesBackslash,
    BaseEscapeSingleQuotesQuote,
)
from dbt.tests.adapter.utils.test_except import BaseExcept
from dbt.tests.adapter.utils.test_intersect import BaseIntersect
from dbt.tests.adapter.utils.test_last_day import BaseLastDay
from dbt.tests.adapter.utils.test_length import BaseLength
from dbt.tests.adapter.utils.test_listagg import BaseListagg
from dbt.tests.adapter.utils.test_position import BasePosition
from dbt.tests.adapter.utils.test_right import BaseRight
from dbt.tests.adapter.utils.test_safe_cast import BaseSafeCast
from dbt.tests.adapter.utils.test_source_freshness_custom_info import BaseCalculateFreshnessMethod
from dbt.tests.adapter.utils.test_string_literal import BaseStringLiteral

# Already covered in tests/functional/adapter/utils/:
#   BaseCurrentTimestamp    → utils/test_utils.py::TestCurrentTimestamp
#   BaseCurrentTimestamps   → utils/test_timestamps.py::TestCurrentTimestampTeradata
#   BaseEquals              → utils/test_equals.py::TestEquals
#   BaseMixedNullCompare    → utils/test_null_compare.py::TestMixedNullCompare
#   BaseNullCompare         → utils/test_null_compare.py::TestNullCompare
#   BaseValidateSqlMethod   → utils/test_validate_sql.py::TestValidateSqlMethod
#   BaseGetPowersOfTwo      → utils/test_utils.py::TestGetPowersOfTwo
#   BaseDateAdd             → utils/test_utils.py::TestDateAdd
#   BaseDateDiff            → utils/test_utils.py::TestDateDiff
#   BaseDateTrunc           → utils/test_utils.py::TestDateTrunc
#   BaseHash                → utils/test_utils.py::TestHash
#   BaseReplace             → utils/test_utils.py::TestReplace
#   BaseGenerateSeries      → utils/test_utils.py::TestGenerateSeries
#   BaseGetIntervalsBetween → utils/test_utils.py::TestGetIntervalsBetween
#   BaseSplitPart           → utils/test_utils.py::TestSplitPart
#   BaseDateSpine           → utils/test_date_spine.py::TestDateSpine


@pytest.mark.skip(reason="Teradata does not support ANY_VALUE() aggregate function on this version")
class TestAnyValueTeradata(BaseAnyValue):
    pass


@pytest.mark.skip(reason="Teradata does not support native ARRAY types")
class TestArrayAppendTeradata(BaseArrayAppend):
    pass


@pytest.mark.skip(reason="Teradata does not support native ARRAY types")
class TestArrayConcatTeradata(BaseArrayConcat):
    pass


@pytest.mark.skip(reason="Teradata does not support native ARRAY types")
class TestArrayConstructTeradata(BaseArrayConstruct):
    pass


@pytest.mark.skip(reason="Teradata has no bool_or aggregate function; no equivalent override")
class TestBoolOrTeradata(BaseBoolOr):
    pass


@pytest.mark.skip(reason="Teradata has no native BOOLEAN type; cast fixture uses types unsupported in Teradata")
class TestCastTeradata(BaseCast):
    pass


@pytest.mark.skip(reason="Teradata has no native BOOLEAN type; cast_bool_to_text not applicable")
class TestCastBoolToTextTeradata(BaseCastBoolToText):
    pass


@pytest.mark.skip(reason="concat fixture causes Error 3798 (column expression larger than max size) in assert_equal comparison")
class TestConcatTeradata(BaseConcat):
    pass


class TestCurrentTimestampAwareTeradata(BaseCurrentTimestampAware):
    def test_current_timestamp_matches_utc(self, project):
        pytest.skip(
            "Teradata server returns local time (not UTC) for CURRENT_TIMESTAMP; "
            "clock skew between server timezone and Python UTC exceeds tolerance."
        )


@pytest.mark.skip(
    reason=(
        "Teradata CURRENT_TIMESTAMP returns timezone-aware values (server local time), "
        "so test_current_timestamp_type (expects naive) and test_current_timestamp_matches_utc "
        "(expects UTC) both fail on this server configuration."
    )
)
class TestCurrentTimestampNaiveTeradata(BaseCurrentTimestampNaive):
    pass


class TestDateTeradata(BaseDate):
    @pytest.fixture(scope="class")
    def models(self):
        # dbt.date_spine() itself compiles to a `WITH rawdata AS (...), ...`
        # statement (Teradata's own generate_series() override already
        # avoids WITH RECURSIVE for this reason - see
        # dbt/include/teradata/macros/utils/generate_series.sql). The
        # upstream test_date.sql model then nests that whole WITH statement
        # inside its own `generated_dates AS (...)` CTE, producing a nested
        # WITH clause, which Teradata rejects with Error 6926 regardless of
        # materialization ("WITH clause ... is not supported within WITH
        # definitions"). Split date_spine() into its own model (so its WITH
        # is top-level) and join to it by ref() instead of nesting.
        # Separately, the upstream expected_dates CTE UNION ALLs literal-only
        # SELECTs with no FROM clause, which Teradata rejects with Error 3888
        # ("A SELECT for a UNION, INTERSECT or MINUS must reference a
        # table"); add a dummy single-row FROM to each branch.
        return {
            "test_date.yml": fixture_date.models__test_date_yml,
            "test_date_spine.sql": (
                "{{ config(materialized='table') }}\n"
                + "{{ dbt.date_spine(\"day\", date(2023, 9, 7), date(2023, 9, 10)) }}"
            ),
            "test_date.sql": """
with expected_dates as (

    select cast('2023-09-07' as date) as expected
    from (select 1 as dbt_dummy) as dbt_dummy
    union all
    select cast('2023-09-08' as date) as expected
    from (select 1 as dbt_dummy) as dbt_dummy
    union all
    select cast('2023-09-09' as date) as expected
    from (select 1 as dbt_dummy) as dbt_dummy

),

joined as (
    select
        generated_dates.date_day,
        expected_dates.expected
    from {{ ref('test_date_spine') }} as generated_dates
    full outer join expected_dates on generated_dates.date_day = expected_dates.expected
)

select * from joined
""",
        }


@pytest.mark.skip(reason="Teradata uses quote-doubling, not backslash escape for single quotes")
class TestEscapeSingleQuotesBackslashTeradata(BaseEscapeSingleQuotesBackslash):
    pass


# Teradata rejects a FROM-less SELECT on either side of a set operator:
#   "[Error 3888] A SELECT for a UNION, INTERSECT or MINUS must reference a
#   table."
# The upstream fixture UNIONs two bare SELECTs, so it needs a dummy FROM to
# parse. That is a fixture-portability issue, not a defect in
# teradata__escape_single_quotes, so fix it here rather than in the adapter.
_ESCAPE_SINGLE_QUOTES_QUOTE_SQL = """
select
  '{{ escape_single_quotes("they're") }}' as actual,
  'they''re' as expected,
  {{ length(string_literal(escape_single_quotes("they're"))) }} as actual_length,
  7 as expected_length
from (select 1 as dbt_dummy) as dbt_escape_src

union all

select
  '{{ escape_single_quotes("they are") }}' as actual,
  'they are' as expected,
  {{ length(string_literal(escape_single_quotes("they are"))) }} as actual_length,
  8 as expected_length
from (select 1 as dbt_dummy) as dbt_escape_src
"""


class TestEscapeSingleQuotesQuoteTeradata(BaseEscapeSingleQuotesQuote):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_escape_single_quotes.yml": fixture_escape_single_quotes.models__test_escape_single_quotes_yml,
            "test_escape_single_quotes.sql": self.interpolate_macro_namespace(
                _ESCAPE_SINGLE_QUOTES_QUOTE_SQL, "escape_single_quotes"
            ),
        }


class TestExceptTeradata(BaseExcept):
    pass


class TestIntersectTeradata(BaseIntersect):
    pass


@pytest.mark.skip(
    reason=(
        "BaseLastDay exercises the 'quarter' datepart, which Teradata's INTERVAL "
        "syntax does not accept: DATE '2020-01-01' + INTERVAL '1' quarter fails "
        "with '[Error 3707] ... expected something like a DAY/HOUR/MINUTE/MONTH "
        "keyword ... and the word quarter' (INTERVAL '1' month works). Enabling "
        "this needs teradata__dateadd to translate quarter into 3 months, not a "
        "fixture change."
    )
)
class TestLastDayTeradata(BaseLastDay):
    pass


class TestLengthTeradata(BaseLength):
    pass


@pytest.mark.skip(
    reason=(
        "Teradata has no LISTAGG function and dbt-teradata does not implement a "
        "teradata__listagg override, so the dispatch falls through to "
        "default__listagg and the emitted LISTAGG(...) fails to parse: "
        "'[Error 3706] Syntax error: Data Type \"string_text\" does not match a "
        "Defined Type name.' Enabling this needs a teradata__listagg macro "
        "(Teradata's idiomatic equivalent is XMLAGG-based aggregation), not a "
        "fixture change."
    )
)
class TestListaggTeradata(BaseListagg):
    pass


# The upstream fixture aliases a column `result`, which Teradata reserves:
#   SELECT 1 AS result   -> [Error 3707] syntax error
#   SELECT 1 AS "result" -> OK
# Quoting the reference is enough; the seed itself loads fine because dbt
# already quotes seed column names.
_POSITION_SQL = """
with data as (

    select * from {{ ref('data_position') }}

)

select

    {{ position('substring_text', 'string_text') }} as actual,
    "result" as expected

from data
"""


class TestPositionTeradata(BasePosition):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_position.yml": fixture_position.models__test_position_yml,
            "test_position.sql": self.interpolate_macro_namespace(
                _POSITION_SQL, "position"
            ),
        }


class TestRightTeradata(BaseRight):
    pass


@pytest.mark.skip(
    reason=(
        "Fixture casts via api.Column.translate_type('string'), which resolves to "
        "'TEXT' through the inherited Column.TYPE_LABELS — not a Teradata type. "
        "Needs a TeradataColumn.TYPE_LABELS override (the dbt-adapters-sanctioned "
        "mechanism) before this can be enabled; teradata__safe_cast itself is "
        "covered by tests/unit/test_safe_cast_macro.py and "
        "test_unit_tests.py::TestSafeCast."
    )
)
class TestSafeCastSuiteTeradata(BaseSafeCast):
    pass


@pytest.mark.skip(
    reason=(
        "The base test's custom freshness SQL builds a derived table whose "
        "expressions are not all aliased, which Teradata rejects: '[Error 3706] "
        "Syntax error: All expressions in a derived table must have an explicit "
        "name.' The SQL is embedded in the base class rather than an overridable "
        "fixture, so enabling this needs an upstream change or a local "
        "reimplementation of the test."
    )
)
class TestCalculateFreshnessMethodTeradata(BaseCalculateFreshnessMethod):
    pass


@pytest.mark.skip(reason="string_literal fixture requires teradata__string_literal macro override; tracked separately")
class TestStringLiteralTeradata(BaseStringLiteral):
    pass
