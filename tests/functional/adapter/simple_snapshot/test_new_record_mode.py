import pytest

from dbt.tests.util import run_dbt, relation_from_name

# -- SQL fixtures -------------------------------------------------------

_seed_new_record_mode = """
create table {schema}.seed (
    id INTEGER,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    email VARCHAR(50),
    gender VARCHAR(50),
    ip_address VARCHAR(20),
    updated_at TIMESTAMP
);
"""

create_snapshot_expected_sql = """
create table {schema}.snapshot_expected (
    id INTEGER,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    email VARCHAR(50),
    gender VARCHAR(50),
    ip_address VARCHAR(20),

    -- snapshotting fields
    updated_at TIMESTAMP,
    dbt_valid_from TIMESTAMP,
    dbt_valid_to   TIMESTAMP,
    dbt_scd_id     BYTE(4),
    dbt_updated_at TIMESTAMP,
    dbt_is_deleted varchar(50)
);
"""

seed_insert_sql = """
insert into {schema}.seed (id, first_name, last_name, email, gender, ip_address, updated_at) values
(1, 'Judith', 'Kennedy', '(not provided)', 'Female', '54.60.24.128', '2015-12-24 12:19:28');
insert into {schema}.seed (id, first_name, last_name, email, gender, ip_address, updated_at) values
(2, 'Arthur', 'Kelly', '(not provided)', 'Male', '62.56.24.215', '2015-10-28 16:22:15');
insert into {schema}.seed (id, first_name, last_name, email, gender, ip_address, updated_at) values
(3, 'Rachel', 'Moreno', 'rmoreno2@msu.edu', 'Female', '31.222.249.23', '2016-04-05 02:05:30');
insert into {schema}.seed (id, first_name, last_name, email, gender, ip_address, updated_at) values
(4, 'Ralph', 'Turner', 'rturner3@hp.com', 'Male', '157.83.76.114', '2016-08-08 00:06:51');
insert into {schema}.seed (id, first_name, last_name, email, gender, ip_address, updated_at) values
(5, 'Laura', 'Gonzales', 'lgonzales4@howstuffworks.com', 'Female', '30.54.105.168', '2016-09-01 08:25:38');
insert into {schema}.seed (id, first_name, last_name, email, gender, ip_address, updated_at) values
(6, 'Katherine', 'Lopez', 'klopez5@yahoo.co.jp', 'Female', '169.138.46.89', '2016-08-30 18:52:11');
insert into {schema}.seed (id, first_name, last_name, email, gender, ip_address, updated_at) values
(7, 'Jeremy', 'Hamilton', 'jhamilton6@mozilla.org', 'Male', '231.189.13.133', '2016-07-17 02:09:46');
insert into {schema}.seed (id, first_name, last_name, email, gender, ip_address, updated_at) values
(8, 'Heather', 'Rose', 'hrose7@goodreads.com', 'Female', '87.165.201.65', '2015-12-29 22:03:56');
insert into {schema}.seed (id, first_name, last_name, email, gender, ip_address, updated_at) values
(9, 'Gregory', 'Kelly', 'gkelly8@trellian.com', 'Male', '154.209.99.7', '2016-03-24 21:18:16');
insert into {schema}.seed (id, first_name, last_name, email, gender, ip_address, updated_at) values
(10, 'Rachel', 'Lopez', 'rlopez9@themeforest.net', 'Female', '237.165.82.71', '2016-08-20 15:44:49');
insert into {schema}.seed (id, first_name, last_name, email, gender, ip_address, updated_at) values
(11, 'Donna', 'Welch', 'dwelcha@shutterfly.com', 'Female', '103.33.110.138', '2016-02-27 01:41:48');
insert into {schema}.seed (id, first_name, last_name, email, gender, ip_address, updated_at) values
(12, 'Russell', 'Lawrence', 'rlawrenceb@qq.com', 'Male', '189.115.73.4', '2016-06-11 03:07:09');
insert into {schema}.seed (id, first_name, last_name, email, gender, ip_address, updated_at) values
(13, 'Michelle', 'Montgomery', 'mmontgomeryc@scientificamerican.com', 'Female', '243.220.95.82', '2016-06-18 16:27:19');
insert into {schema}.seed (id, first_name, last_name, email, gender, ip_address, updated_at) values
(14, 'Walter', 'Castillo', 'wcastillod@pagesperso-orange.fr', 'Male', '71.159.238.196', '2016-10-06 01:55:44');
insert into {schema}.seed (id, first_name, last_name, email, gender, ip_address, updated_at) values
(15, 'Robin', 'Mills', 'rmillse@vkontakte.ru', 'Female', '172.190.5.50', '2016-10-31 11:41:21');
insert into {schema}.seed (id, first_name, last_name, email, gender, ip_address, updated_at) values
(16, 'Raymond', 'Holmes', 'rholmesf@usgs.gov', 'Male', '148.153.166.95', '2016-10-03 08:16:38');
insert into {schema}.seed (id, first_name, last_name, email, gender, ip_address, updated_at) values
(17, 'Gary', 'Bishop', 'gbishopg@plala.or.jp', 'Male', '161.108.182.13', '2016-08-29 19:35:20');
insert into {schema}.seed (id, first_name, last_name, email, gender, ip_address, updated_at) values
(18, 'Anna', 'Riley', 'arileyh@nasa.gov', 'Female', '253.31.108.22', '2015-12-11 04:34:27');
insert into {schema}.seed (id, first_name, last_name, email, gender, ip_address, updated_at) values
(19, 'Sarah', 'Knight', 'sknighti@foxnews.com', 'Female', '222.220.3.177', '2016-09-26 00:49:06');
insert into {schema}.seed (id, first_name, last_name, email, gender, ip_address, updated_at) values
(20, 'Phyllis', 'Fox', null, 'Female', '163.191.232.95', '2016-08-21 10:35:19');
"""

populate_snapshot_expected_sql = """
insert into {schema}.snapshot_expected (
    id, first_name, last_name, email, gender, ip_address,
    updated_at, dbt_valid_from, dbt_valid_to, dbt_updated_at, dbt_scd_id, dbt_is_deleted
)
select
    id, first_name, last_name, email, gender, ip_address,
    updated_at,
    updated_at as dbt_valid_from,
    cast(null as timestamp) as dbt_valid_to,
    updated_at as dbt_updated_at,
    HASHROW(coalesce(cast(id || '-' || first_name as varchar(50)), '')
        || '|' || coalesce(cast(updated_at as varchar(50)), '')) as dbt_scd_id,
    'False' as dbt_is_deleted
from {schema}.seed;
"""

_snapshot_actual_sql = """
{% snapshot snapshot_actual %}

    {{
        config(
            unique_key='id || ' ~ "'-'" ~ ' || first_name',
        )
    }}

    select * from {{target.schema}}.seed

{% endsnapshot %}
"""

_snapshots_yml = """
snapshots:
  - name: snapshot_actual
    config:
      strategy: timestamp
      updated_at: updated_at
      hard_deletes: new_record
"""

_ref_snapshot_sql = """
select * from {{ ref('snapshot_actual') }}
"""

# `unique_key` here is a list (composite key), unlike `_snapshot_actual_sql` above which uses
# a string expression. This exercises the `strategy.unique_key | is_list` branch of the
# `deletion_records` CTE in teradata__snapshot_staging_table (dbt/include/teradata/macros/
# materializations/snapshot/helpers.sql), i.e. `snapshotted_data.dbt_unique_key_{{ loop.index }}`
# and the multi-column `new_scd_id` hashing, which is otherwise untested.
_snapshot_actual_composite_key_sql = """
{% snapshot snapshot_actual %}

    {{
        config(
            unique_key=['id', 'first_name'],
        )
    }}

    select * from {{target.schema}}.seed

{% endsnapshot %}
"""

_snapshots_composite_key_yml = """
snapshots:
  - name: snapshot_actual
    config:
      strategy: timestamp
      updated_at: updated_at
      hard_deletes: new_record
"""

_invalidate_sql = """
update {schema}.seed set
    updated_at = updated_at + interval '1' hour,
    email      = case when id = 20 then 'pfoxj@creativecommons.org' else 'new_' || email end
where id >= 10 and id <= 20;

update {schema}.snapshot_expected set
    dbt_valid_to   = updated_at + interval '1' hour
where id >= 10 and id <= 20;
"""

_update_sql = """
insert into {schema}.snapshot_expected (
    id, first_name, last_name, email, gender, ip_address,
    updated_at, dbt_valid_from, dbt_valid_to, dbt_updated_at, dbt_scd_id, dbt_is_deleted
)
select
    id, first_name, last_name, email, gender, ip_address,
    updated_at,
    updated_at as dbt_valid_from,
    cast(null as timestamp) as dbt_valid_to,
    updated_at as dbt_updated_at,
    HASHROW(coalesce(cast(id || '-' || first_name as varchar(50)), '')
        || '|' || coalesce(cast(updated_at as varchar(50)), '')) as dbt_scd_id,
    'False' as dbt_is_deleted
from {schema}.seed
where id >= 10 and id <= 20;
"""

_delete_sql = """
delete from {schema}.seed where id = 1
"""

# -- Helper -------------------------------------------------------

def _drop_table_safe(project, table_name):
    """Drop a table if it exists."""
    relation = relation_from_name(project.adapter, table_name)
    try:
        project.run_sql(f"DROP TABLE /*+ IF EXISTS */ {relation}")
    except Exception as ex:
        # Teradata adapter suppresses "does not exist" errors for /*+ IF EXISTS */
        # (errors 3807, 3854, 3853, 7825, 6321), but in test context via project.run_sql()
        # we still need explicit handling for Error 3807
        if "[Error 3807]" not in str(ex):
            raise


def _reset_tables(project):
    """Drop seed and snapshot tables so each test starts clean."""
    _drop_table_safe(project, "seed")
    _drop_table_safe(project, "snapshot_actual")
    _drop_table_safe(project, "snapshot_expected")


def _get_snapshot_rows(project, columns="*", where="1=1"):
    """Return rows from the snapshot_actual table."""
    relation = relation_from_name(project.adapter, "snapshot_actual")
    return project.run_sql(
        f"select {columns} from {relation} where {where}", fetch="all"
    )


def _assert_snapshot_success(results):
    """Assert that exactly one snapshot ran and it succeeded.

    Teradata snapshots return 'activity: Insert, rows_affected: N' rather than
    'success', so we accept both to support all snapshot materialization outcomes.
    """
    assert len(results) == 1
    status = str(results[0].status)
    assert status == "success" or status.startswith("activity:"), \
        f"Unexpected snapshot status: {status}"


# -- Test class -------------------------------------------------------

class SnapshotNewRecordMode:
    @pytest.fixture(scope="class")
    def snapshots(self):
        return {"snapshot.sql": _snapshot_actual_sql}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "snapshots.yml": _snapshots_yml,
            "ref_snapshot.sql": _ref_snapshot_sql,
        }

    @pytest.fixture(scope="class")
    def invalidate_sql(self):
        return _invalidate_sql

    @pytest.fixture(scope="class")
    def update_sql(self):
        return _update_sql

    @pytest.fixture(scope="class")
    def delete_sql(self):
        return _delete_sql

    def test_snapshot_new_record_mode(self, project, invalidate_sql, update_sql):
        """Test initial load + updates with expected-table comparison."""
        _reset_tables(project)
        project.run_sql(_seed_new_record_mode)
        project.run_sql(create_snapshot_expected_sql)
        project.run_sql(seed_insert_sql)
        project.run_sql(populate_snapshot_expected_sql)

        # --- Run 1: initial snapshot load ---
        results = run_dbt(["snapshot"])
        _assert_snapshot_success(results)

        # --- Run 2: update records 10-20 and re-snapshot ---
        project.run_sql(invalidate_sql)
        project.run_sql(update_sql)

        results = run_dbt(["snapshot"])
        _assert_snapshot_success(results)

        # Verify snapshot matches expected table (bidirectional MINUS)
        relation_actual = relation_from_name(project.adapter, "snapshot_actual")
        relation_expected = relation_from_name(project.adapter, "snapshot_expected")

        cols = "id, first_name, last_name, email, gender, ip_address, updated_at, dbt_valid_from, dbt_valid_to, dbt_scd_id, dbt_updated_at, dbt_is_deleted"

        result = project.run_sql(
            f"select {cols} from {relation_actual} minus select {cols} from {relation_expected}",
            fetch="one",
        )
        assert result is None, f"Rows in actual but not expected: {result}"

        result2 = project.run_sql(
            f"select {cols} from {relation_expected} minus select {cols} from {relation_actual}",
            fetch="one",
        )
        assert result2 is None, f"Rows in expected but not actual: {result2}"

        # --- Run 3: delete record id=1 and snapshot ---
        project.run_sql(_delete_sql)

        results = run_dbt(["snapshot"])
        _assert_snapshot_success(results)

    def test_hard_delete_creates_new_record_with_correct_flags(self, project):
        """After deleting a source record, the snapshot should contain:
        - The original record with dbt_valid_to set (closed) and dbt_is_deleted='False'
        - A new deletion record with dbt_is_deleted='True' and dbt_valid_to IS NULL
        """
        _reset_tables(project)
        project.run_sql(_seed_new_record_mode)
        project.run_sql(seed_insert_sql)

        # Initial snapshot
        results = run_dbt(["snapshot"])
        _assert_snapshot_success(results)

        # All 20 records should be active (dbt_valid_to IS NULL, dbt_is_deleted='False')
        rows = _get_snapshot_rows(project, "count(*)", "dbt_valid_to is null and dbt_is_deleted = 'False'")
        assert rows[0][0] == 20, f"Expected 20 active records, got {rows[0][0]}"

        # Delete record id=1
        project.run_sql(_delete_sql)

        # Snapshot after delete
        results = run_dbt(["snapshot"])
        _assert_snapshot_success(results)

        # Original record for id=1 should now be closed (dbt_valid_to IS NOT NULL)
        closed_rows = _get_snapshot_rows(
            project,
            "count(*)",
            "id = 1 and dbt_valid_to is not null and dbt_is_deleted = 'False'",
        )
        assert closed_rows[0][0] == 1, \
            f"Expected 1 closed original record for id=1, got {closed_rows[0][0]}"

        # A new deletion record should exist with dbt_is_deleted='True'
        deleted_rows = _get_snapshot_rows(
            project,
            "count(*)",
            "id = 1 and dbt_is_deleted = 'True'",
        )
        assert deleted_rows[0][0] == 1, \
            f"Expected 1 deletion record for id=1, got {deleted_rows[0][0]}"

    def test_hard_delete_produces_unique_scd_id(self, project):
        """The deletion record must have a different dbt_scd_id than the original record."""
        _reset_tables(project)
        project.run_sql(_seed_new_record_mode)
        project.run_sql(seed_insert_sql)

        results = run_dbt(["snapshot"])
        _assert_snapshot_success(results)

        # Delete record id=1
        project.run_sql(_delete_sql)

        results = run_dbt(["snapshot"])
        _assert_snapshot_success(results)

        # Fetch original and deletion rows separately to avoid relying on result ordering
        original_rows = _get_snapshot_rows(project, "dbt_scd_id", "id = 1 and dbt_is_deleted = 'False'")
        deleted_rows = _get_snapshot_rows(project, "dbt_scd_id", "id = 1 and dbt_is_deleted = 'True'")
        assert len(original_rows) == 1, f"Expected 1 original record for id=1, got {len(original_rows)}"
        assert len(deleted_rows) == 1, f"Expected 1 deletion record for id=1, got {len(deleted_rows)}"

        scd_id_original = original_rows[0][0]
        scd_id_deleted = deleted_rows[0][0]
        assert scd_id_original != scd_id_deleted, \
            f"dbt_scd_id must be unique: original={scd_id_original}, deletion={scd_id_deleted}"

        # Verify global uniqueness — no duplicate dbt_scd_id in the entire snapshot
        relation = relation_from_name(project.adapter, "snapshot_actual")
        dups = project.run_sql(
            f"select dbt_scd_id, count(*) as cnt from {relation} group by dbt_scd_id having count(*) > 1",
            fetch="all",
        )
        assert len(dups) == 0, f"Found duplicate dbt_scd_id values: {dups}"

    def test_snapshot_idempotent_after_delete(self, project):
        """Running snapshot multiple times after a delete should NOT create duplicate records."""
        _reset_tables(project)
        project.run_sql(_seed_new_record_mode)
        project.run_sql(seed_insert_sql)

        # Initial snapshot
        results = run_dbt(["snapshot"])
        _assert_snapshot_success(results)

        # Delete record id=1
        project.run_sql(_delete_sql)

        # First snapshot after delete
        results = run_dbt(["snapshot"])
        _assert_snapshot_success(results)

        # Count total records
        count_after_first = _get_snapshot_rows(project, "count(*)")
        total_after_first = count_after_first[0][0]

        # Run snapshot 3 more times — count should NOT change
        for _ in range(3):
            results = run_dbt(["snapshot"])
            _assert_snapshot_success(results)

        count_after_repeats = _get_snapshot_rows(project, "count(*)")
        total_after_repeats = count_after_repeats[0][0]

        assert total_after_first == total_after_repeats, (
            f"Record count changed from {total_after_first} to {total_after_repeats} "
            f"after 3 idempotent snapshot runs — exponential duplication bug!"
        )

    def test_non_deleted_records_unaffected(self, project):
        """Records that were NOT deleted should remain unchanged after delete + snapshot."""
        _reset_tables(project)
        project.run_sql(_seed_new_record_mode)
        project.run_sql(seed_insert_sql)

        results = run_dbt(["snapshot"])
        _assert_snapshot_success(results)

        # Delete only id=1 (Judith)
        project.run_sql(_delete_sql)

        results = run_dbt(["snapshot"])
        _assert_snapshot_success(results)

        # All other 19 records should still be active with dbt_is_deleted='False'
        active_rows = _get_snapshot_rows(
            project,
            "count(*)",
            "id <> 1 and dbt_valid_to is null and dbt_is_deleted = 'False'",
        )
        assert active_rows[0][0] == 19, \
            f"Expected 19 unaffected active records, got {active_rows[0][0]}"

        # No other record should have dbt_is_deleted='True'
        other_deleted = _get_snapshot_rows(
            project,
            "count(*)",
            "id <> 1 and dbt_is_deleted = 'True'",
        )
        assert other_deleted[0][0] == 0, \
            f"Expected 0 deletion records for non-deleted sources, got {other_deleted[0][0]}"


class TestSnapshotNewRecordModeTeradata(SnapshotNewRecordMode):
    pass


class SnapshotNewRecordModeCompositeKey:
    """
    Same `hard_deletes: new_record` behavior as `SnapshotNewRecordMode`, but with a
    composite/list `unique_key` (['id', 'first_name']) instead of a string expression.
    This is the code path a reviewer flagged in PR #241: the `deletion_records` CTE
    references `snapshotted_data.dbt_unique_key_{{ loop.index }}` directly instead of
    re-deriving it from the raw key column, relying on the `snapshotted_data` CTE already
    exposing those columns (via the shared `unique_key_fields` macro). These tests confirm
    that assumption holds end-to-end on a live Teradata instance.
    """

    @pytest.fixture(scope="class")
    def snapshots(self):
        return {"snapshot.sql": _snapshot_actual_composite_key_sql}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "snapshots.yml": _snapshots_composite_key_yml,
            "ref_snapshot.sql": _ref_snapshot_sql,
        }

    def test_hard_delete_creates_new_record_with_correct_flags(self, project):
        """After deleting a source record, the snapshot should contain:
        - The original record with dbt_valid_to set (closed) and dbt_is_deleted='False'
        - A new deletion record with dbt_is_deleted='True' and dbt_valid_to IS NULL
        """
        _reset_tables(project)
        project.run_sql(_seed_new_record_mode)
        project.run_sql(seed_insert_sql)

        # Initial snapshot
        results = run_dbt(["snapshot"])
        _assert_snapshot_success(results)

        # All 20 records should be active (dbt_valid_to IS NULL, dbt_is_deleted='False')
        rows = _get_snapshot_rows(project, "count(*)", "dbt_valid_to is null and dbt_is_deleted = 'False'")
        assert rows[0][0] == 20, f"Expected 20 active records, got {rows[0][0]}"

        # Delete record id=1
        project.run_sql(_delete_sql)

        # Snapshot after delete
        results = run_dbt(["snapshot"])
        _assert_snapshot_success(results)

        # Original record for id=1 should now be closed (dbt_valid_to IS NOT NULL)
        closed_rows = _get_snapshot_rows(
            project,
            "count(*)",
            "id = 1 and dbt_valid_to is not null and dbt_is_deleted = 'False'",
        )
        assert closed_rows[0][0] == 1, \
            f"Expected 1 closed original record for id=1, got {closed_rows[0][0]}"

        # A new deletion record should exist with dbt_is_deleted='True'
        deleted_rows = _get_snapshot_rows(
            project,
            "count(*)",
            "id = 1 and dbt_is_deleted = 'True'",
        )
        assert deleted_rows[0][0] == 1, \
            f"Expected 1 deletion record for id=1, got {deleted_rows[0][0]}"

    def test_hard_delete_produces_unique_scd_id(self, project):
        """The deletion record must have a different dbt_scd_id than the original record,
        with the scd_id computed from the composite dbt_unique_key_1 / dbt_unique_key_2 columns."""
        _reset_tables(project)
        project.run_sql(_seed_new_record_mode)
        project.run_sql(seed_insert_sql)

        results = run_dbt(["snapshot"])
        _assert_snapshot_success(results)

        # Delete record id=1
        project.run_sql(_delete_sql)

        results = run_dbt(["snapshot"])
        _assert_snapshot_success(results)

        # Fetch original and deletion rows separately to avoid relying on result ordering
        original_rows = _get_snapshot_rows(project, "dbt_scd_id", "id = 1 and dbt_is_deleted = 'False'")
        deleted_rows = _get_snapshot_rows(project, "dbt_scd_id", "id = 1 and dbt_is_deleted = 'True'")
        assert len(original_rows) == 1, f"Expected 1 original record for id=1, got {len(original_rows)}"
        assert len(deleted_rows) == 1, f"Expected 1 deletion record for id=1, got {len(deleted_rows)}"

        scd_id_original = original_rows[0][0]
        scd_id_deleted = deleted_rows[0][0]
        assert scd_id_original != scd_id_deleted, \
            f"dbt_scd_id must be unique: original={scd_id_original}, deletion={scd_id_deleted}"

        # Verify global uniqueness — no duplicate dbt_scd_id in the entire snapshot
        relation = relation_from_name(project.adapter, "snapshot_actual")
        dups = project.run_sql(
            f"select dbt_scd_id, count(*) as cnt from {relation} group by dbt_scd_id having count(*) > 1",
            fetch="all",
        )
        assert len(dups) == 0, f"Found duplicate dbt_scd_id values: {dups}"

    def test_snapshot_idempotent_after_delete(self, project):
        """Running snapshot multiple times after a delete should NOT create duplicate records,
        with a composite unique_key."""
        _reset_tables(project)
        project.run_sql(_seed_new_record_mode)
        project.run_sql(seed_insert_sql)

        # Initial snapshot
        results = run_dbt(["snapshot"])
        _assert_snapshot_success(results)

        # Delete record id=1
        project.run_sql(_delete_sql)

        # First snapshot after delete
        results = run_dbt(["snapshot"])
        _assert_snapshot_success(results)

        # Count total records
        count_after_first = _get_snapshot_rows(project, "count(*)")
        total_after_first = count_after_first[0][0]

        # Run snapshot 3 more times — count should NOT change
        for _ in range(3):
            results = run_dbt(["snapshot"])
            _assert_snapshot_success(results)

        count_after_repeats = _get_snapshot_rows(project, "count(*)")
        total_after_repeats = count_after_repeats[0][0]

        assert total_after_first == total_after_repeats, (
            f"Record count changed from {total_after_first} to {total_after_repeats} "
            f"after 3 idempotent snapshot runs — exponential duplication bug!"
        )

    def test_non_deleted_records_unaffected(self, project):
        """Records that were NOT deleted should remain unchanged after delete + snapshot."""
        _reset_tables(project)
        project.run_sql(_seed_new_record_mode)
        project.run_sql(seed_insert_sql)

        results = run_dbt(["snapshot"])
        _assert_snapshot_success(results)

        # Delete only id=1 (Judith)
        project.run_sql(_delete_sql)

        results = run_dbt(["snapshot"])
        _assert_snapshot_success(results)

        # All other 19 records should still be active with dbt_is_deleted='False'
        active_rows = _get_snapshot_rows(
            project,
            "count(*)",
            "id <> 1 and dbt_valid_to is null and dbt_is_deleted = 'False'",
        )
        assert active_rows[0][0] == 19, \
            f"Expected 19 unaffected active records, got {active_rows[0][0]}"

        # No other record should have dbt_is_deleted='True'
        other_deleted = _get_snapshot_rows(
            project,
            "count(*)",
            "id <> 1 and dbt_is_deleted = 'True'",
        )
        assert other_deleted[0][0] == 0, \
            f"Expected 0 deletion records for non-deleted sources, got {other_deleted[0][0]}"


class TestSnapshotNewRecordModeCompositeKeyTeradata(SnapshotNewRecordModeCompositeKey):
    pass
