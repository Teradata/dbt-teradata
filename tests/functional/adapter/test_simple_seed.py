import re

import pytest

from dbt.tests.adapter.simple_seed.test_seed import (
    BaseBasicSeedTests,
    BaseSeedConfigFullRefreshOff,
    BaseSeedConfigFullRefreshOn,
    BaseSeedCustomSchema,
    BaseSeedParsing,
    BaseSeedSpecificFormats,
    BaseSeedWithEmptyDelimiter,
    BaseSeedWithUniqueDelimiter,
    BaseSeedWithWrongDelimiter,
    BaseSimpleSeedEnabledViaConfig,
    BaseSimpleSeedWithBOM,
    BaseTestEmptySeed,
)
from dbt.tests.adapter.simple_seed import seeds as base_seeds
from dbt.tests.adapter.simple_seed.test_seed_type_override import BaseSimpleSeedColumnOverride

# Classes below that use setUp(project.run_sql(seeds.seeds__expected_sql)) fail because
# seeds__expected_sql uses the TEXT data type which Teradata does not support (Error 3706).
# Seed functionality is tested end-to-end via dbt_project functional tests.
_SKIP_REASON = "setUp SQL uses TEXT data type not supported in Teradata (Error 3706)"


@pytest.mark.skip(reason=_SKIP_REASON)
class TestBasicSeedTeradata(BaseBasicSeedTests):
    pass


@pytest.mark.skip(reason=_SKIP_REASON)
class TestSeedConfigFullRefreshOffTeradata(BaseSeedConfigFullRefreshOff):
    pass


@pytest.mark.skip(reason=_SKIP_REASON)
class TestSeedConfigFullRefreshOnTeradata(BaseSeedConfigFullRefreshOn):
    pass


@pytest.mark.skip(reason=_SKIP_REASON)
class TestSeedCustomSchemaTeradata(BaseSeedCustomSchema):
    pass


@pytest.mark.skip(reason=_SKIP_REASON)
class TestSeedParsingTeradata(BaseSeedParsing):
    pass


class TestSeedSpecificFormatsTeradata(BaseSeedSpecificFormats):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        # seed_unicode.csv contains a non-Latin character (U+2013 EN DASH).
        # Teradata's inferred VARCHAR column type defaults to the LATIN
        # character set, which rejects it with Error 6706 ("untranslatable
        # character"). Force UNICODE for that seed's column.
        return {
            "seeds": {
                "test": {
                    "seed_unicode": {"+column_types": {"seed_id": "VARCHAR(64) CHARACTER SET UNICODE"}},
                },
            },
        }

    @pytest.fixture(scope="module")
    def test_data_dir(self, tmp_path_factory):
        # The upstream fixture defaults to "<this test file's directory>/data",
        # which collides with tests/functional/adapter/data/ -- a directory
        # this repo already commits (seed_model.sql / seed_run.sql, used by
        # test_hooks.py). The base class's seeds() fixture mkdir()s and later
        # rm_dir()s test_data_dir, which would fail immediately (the
        # directory already exists) or, worse, delete those committed files.
        # Use an isolated temp directory instead. mktemp() itself creates the
        # returned directory, so nest one level further down so the base
        # class's own mkdir() call still has something to create.
        base = tmp_path_factory.mktemp("seed_specific_formats")
        return base / "data"


@pytest.mark.skip(reason=_SKIP_REASON)
class TestSeedWithEmptyDelimiterTeradata(BaseSeedWithEmptyDelimiter):
    pass


@pytest.mark.skip(reason=_SKIP_REASON)
class TestSeedWithUniqueDelimiterTeradata(BaseSeedWithUniqueDelimiter):
    pass


@pytest.mark.skip(reason=_SKIP_REASON)
class TestSeedWithWrongDelimiterTeradata(BaseSeedWithWrongDelimiter):
    pass


@pytest.mark.skip(reason=_SKIP_REASON)
class TestSimpleSeedEnabledViaConfigTeradata(BaseSimpleSeedEnabledViaConfig):
    pass


@pytest.mark.skip(reason=_SKIP_REASON)
class TestSimpleSeedWithBOMTeradata(BaseSimpleSeedWithBOM):
    pass


class TestEmptySeedTeradata(BaseTestEmptySeed):
    pass


class TestSimpleSeedColumnOverrideTeradata(BaseSimpleSeedColumnOverride):
    @pytest.fixture(scope="class")
    def models(self):
        # The upstream schema.yml asserts Postgres-style information_schema
        # type names (text, boolean, timestamp without time zone). Teradata's
        # adapter reports its own native type names, so the expectations must
        # be adjusted to match what get_columns_in_relation() actually returns.
        return {
            "schema.yml": """
version: 2
seeds:
- name: seed_enabled
  columns:
  - name: birthday
    data_tests:
    - column_type:
        type: DATE
  - name: seed_id
    data_tests:
    - column_type:
        type: character varying(255)

- name: seed_tricky
  columns:
  - name: seed_id
    data_tests:
    - column_type:
        type: INTEGER
  - name: seed_id_str
    data_tests:
    - column_type:
        type: character varying(255)
  - name: a_bool
    data_tests:
    - column_type:
        type: BYTEINT
  - name: looks_like_a_bool
    data_tests:
    - column_type:
        type: character varying(255)
  - name: a_date
    data_tests:
    - column_type:
        type: TIMESTAMP
  - name: looks_like_a_date
    data_tests:
    - column_type:
        type: character varying(255)
  - name: relative
    data_tests:
    - column_type:
        type: character varying(64000)
  - name: weekday
    data_tests:
    - column_type:
        type: character varying(64000)
""".strip()
        }

    @pytest.fixture(scope="class")
    def macros(self):
        # Teradata has no boolean literal type, so the upstream macro's
        # "from (select true) as nothing" fails with Error 3822 ("Cannot
        # resolve column 'true'"). Swap in an equivalent integer literal.
        return {
            "schema_test.sql": """
{% test column_type(model, column_name, type) %}

    {% set cols = adapter.get_columns_in_relation(model) %}

    {% set col_types = {} %}
    {% for col in cols %}
        {% do col_types.update({col.name: col.data_type}) %}
    {% endfor %}

    {% set validation_message = 'Got a column type of ' ~ col_types.get(column_name) ~ ', expected ' ~ type %}

    {% set val = 0 if col_types.get(column_name) == type else 1 %}
    {% if val == 1 and execute %}
        {{ log(validation_message, info=True) }}
    {% endif %}

    select '{{ validation_message }}' as validation_error
    from (select 1 as nothing) as nothing
    where {{ val }} = 1

{% endtest %}
""".strip()
        }

    @pytest.fixture(scope="class")
    def seeds(self):
        # The upstream seed_enabled.csv birthday values are full timestamps
        # (e.g. "2008-09-12 19:08:31"). Postgres silently truncates the time
        # portion when casting a timestamp string to DATE, but Teradata does
        # not (Error 2666: Invalid date supplied), so strip the time portion
        # here to keep the "date" column-type override (and its matching
        # column_type: date schema test) valid on Teradata.
        seed_enabled_csv = re.sub(
            r"( \d{2}:\d{2}:\d{2})$",
            "",
            base_seeds.seeds__enabled_in_config_csv,
            flags=re.MULTILINE,
        )
        return {
            "seed_enabled.csv": seed_enabled_csv,
            "seed_disabled.csv": base_seeds.seeds__disabled_in_config_csv,
            "seed_tricky.csv": base_seeds.seeds__tricky_csv,
        }

    @staticmethod
    def seed_enabled_types():
        return {
            "seed_id": "VARCHAR(255)",
            "birthday": "date",
        }

    @staticmethod
    def seed_tricky_types():
        return {
            "seed_id_str": "VARCHAR(255)",
            "looks_like_a_bool": "VARCHAR(255)",
            "looks_like_a_date": "VARCHAR(255)",
        }
