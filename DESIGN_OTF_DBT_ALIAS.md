# Design Note — dbt `alias` Support for OTF Tables (dbt-teradata)

**Story:** IDE-26154 (branch `IDE-26154_MT255026`)
**Author:** Mohan Talla
**Scope:** Implement / verify dbt's **`alias` resource config** for External OTF
(Iceberg / Delta Lake) table materializations.
**Status:** Design — for review.

---

## 0. CRITICAL Disambiguation — read this first

The word "alias" means **two completely different things** in this area. This
design is about **(A)** only.

| | **(A) dbt `alias` config** — IN SCOPE | **(B) Teradata `CREATE ALIAS TABLE`** — OUT OF SCOPE |
|---|---|---|
| What it is | A dbt resource config that overrides the **relation identifier** (the object name dbt writes). | A Teradata DDL feature that creates a **2-dot, read-only pointer** to a 3-dot OTF table. |
| Effect | Model `sales.sql` with `+alias: sales_dashboard` ⇒ dbt creates the OTF table as `"dl"."otf_db"."sales_dashboard"` instead of `…."sales"`. | `CREATE ALIAS TABLE db.orders, DATALAKE=dl, DATALAKE TABLE=catdb.orders;` ⇒ lets clients read `db.orders` (2-dot) instead of `dl.catdb.orders` (3-dot). |
| Source | [docs.getdbt.com/reference/resource-configs/alias](https://docs.getdbt.com/reference/resource-configs/alias) | *Deltalake using Alias Name — SIT Test Scenarios* PDF; OTF User Guide §3.4.10 |
| Generated DDL | Standard `CREATE TABLE … AS` with a different table name. **No new DDL keyword.** | `CREATE ALIAS TABLE` / `DROP ALIAS TABLE`. |

> **The user has explicitly stated they are *not* implementing Teradata's
> ALIAS TABLE.** The SIT PDF and the OTF-DML memory (`CREATE ALIAS TABLE`,
> read-only, no DML) are reference material for **compatibility awareness only**
> — see §6. Note that the backlog title for IDE-26154 ("Alias-table support for
> External OTF tables") historically referred to **(B)**; this design
> deliberately re-scopes the story to **(A)**. Confirm with the team that this
> re-scope is intended before merge.

---

## 1. What dbt `alias` must do

Per the dbt docs:

- The **identifier** (table name) of a model defaults to its **filename**.
- When `alias` is set (via `{{ config(alias=...) }}`, model `properties.yml`
  `config.alias`, or `dbt_project.yml` `+alias`), the identifier becomes the
  alias value.
- Full relation path is `database.schema.identifier`. For Teradata OTF this
  maps to `"<datalake>"."<otf_database>"."<identifier>"`.
- Alias generation is centralized in the `generate_alias_name` macro; the
  default honors `custom_alias_name` then `node.version` then `node.name`.
- `ref('model_name')` always resolves by **model name**, never by alias — so
  downstream models must keep `ref()`-ing the original name while the physical
  OTF object carries the alias.

For OTF specifically, "alias works" means **all five** of these are true:

1. `dbt run` of an aliased OTF model creates `"dl"."otf_db"."<alias>"`.
2. Incremental re-runs append to `"dl"."otf_db"."<alias>"` (existence probe,
   staging, and INSERT all target the alias).
3. `ref()` from another model to an aliased OTF model compiles to the 3-part
   name using the **alias**.
4. `--full-refresh` DROP+CREATE targets the alias name.
5. Identifier-length and quoting rules hold for the aliased name.

---

## 2. Current-state analysis (how the identifier flows today)

**Key finding: the alias already propagates into the OTF object name** with the
code as it stands, because every OTF code path keys off `relation.identifier`,
and `identifier` *is* the alias.

Trace:

- dbt-core computes `node.alias` at parse time via `generate_alias_name`.
- `RelationConfig.identifier` is a **property returning `self.alias`**
  (`dbt/artifacts/resources/v1/components.py:197-209`).
- `BaseRelation.create_from(...)` passes `identifier=relation_config.identifier`
  straight through (`dbt/adapters/base/relation.py:374-381`).
- `TeradataRelation.create_from` (relation.py:67-83) builds the OTF relation
  with `identifier=relation_config.identifier` → **alias-aware**.
- Table materialization (`table.sql:33`) uses `this.incorporate(type='table')`;
  `this.identifier` is alias-aware.
- `create_otf_table_as.sql` builds the name from `relation.identifier`
  (`teradata__build_otf_relation_name`, line 31-34, called at 84/150).
- Incremental (`incremental_otf.sql`) uses `target_relation.identifier` for the
  existence probe (line 101-104), the staging relation (line 128-132, now via
  `make_temp_relation`), and the final INSERT.

The adapter **only overrides `generate_database_name`** (returns `None`,
adapters.sql:350) — it does **not** override `generate_alias_name`, so dbt's
default alias resolution is in effect and correct. No override is needed.

**Conclusion:** This is primarily a **verification + edge-case hardening +
documentation** task, not a large new code path. The risk is in the edges
below, not in the happy path.

---

## 3. Gap analysis & edge cases

| # | Edge case | Current behavior | Action |
|---|-----------|------------------|--------|
| G1 | **Identifier length > 128** for the *target* OTF object | OTF name = `"dl"."db"."<alias>"`; long alias is sent to the catalog as-is. Teradata BFS limit is 128 chars; OTF/Iceberg catalog limits may differ by catalog (Glue/Unity/Hive). | Verify catalog behavior; if a clear limit exists, add a compiler-error guardrail with a clear message. Otherwise document. |
| G2 | **Staging table name** in incremental = `make_temp_relation(<alias>)` ⇒ `<alias>__dbt_tmp` in the **regular** target schema | Commit `0e82350` switched to `make_temp_relation`, which already applies dbt's name shortening/suffix handling. Long alias + `__dbt_tmp` could still approach 128. | Confirm `make_temp_relation` truncation is sufficient; add a functional test with a long alias. |
| G3 | **`ref()` to an aliased OTF model** | Resolves via manifest → relation identifier = alias → 3-part name uses alias. Should be correct but **untested**. | Add functional/compile test. |
| G4 | **`source()` with `database != schema` heuristic** (relation.py:99-103) | Uses whatever identifier the source declares; alias config does not apply to sources. | No change; document that alias is a *model/seed/snapshot* config, not a source config. |
| G5 | **Existence probe** `adapter.otf_relation_exists(dl, db, identifier)` | Uses alias identifier → correct table is probed. | Covered by G3/G6 tests. |
| G6 | **`--full-refresh`** path | `should_full_refresh()` → `teradata__create_otf_table_as(target_relation, …)` with alias identifier → DROP+CREATE on alias. | Add test. |
| G7 | **Test-harness cleanup** | `_OTF_TEST_TABLES` in `test_otf_integration.py` lists **model names**; aliased objects use the **alias** name and would leak. | New alias tests must register the **alias** name for DROP cleanup. |
| G8 | **Collision: two models with the same alias** in the same OTF database | dbt-core already raises a duplicate-relation error at parse; OTF path inherits this. | No action; note in docs. |
| G9 | **`generate_alias_name` custom override by users** | Default flows through; a user-defined `generate_alias_name` would too. | No action; document compatibility. |

There is **no expected change** to `catalogs.py`, `relation.py`, or the OTF
materialization macros for the happy path. Changes, if any, are limited to:
- an **optional length guardrail** (G1/G2), and
- **tests + docs**.

---

## 4. Proposed implementation

### 4.1 Code (minimal)

1. **(Optional, pending G1 verification)** Add a length/validity guardrail for
   the OTF identifier in `teradata__build_otf_relation_name` (or a small helper
   called from `teradata__create_datalake_table_as` and `teradata__incremental_otf`):
   - If the resolved alias identifier exceeds the catalog's max length, raise
     `exceptions.raise_compiler_error` with a message naming the alias and limit.
   - Only add this if §6/G1 confirms a hard limit; otherwise skip to avoid
     false rejections.

2. **No change** to `generate_alias_name` (default is correct).
3. **No change** to the relation/identifier plumbing (already alias-aware).

### 4.2 Documentation

- README / OTF section: add an "Aliasing OTF tables" subsection showing
  `{{ config(materialized='table', catalog_name='…', alias='…') }}` and the
  resulting 3-part object name; explicitly state this is dbt `alias`, **not**
  Teradata `CREATE ALIAS TABLE`.
- Note that `ref()` still uses the model name.
- Note alias applies to OTF `table` and `incremental` materializations.

### 4.3 CHANGELOG / version

- Add a changelog entry; bump version via `bump2version` per repo convention.

---

## 5. Test plan

### 5.1 Unit tests (no DB) — `tests/unit/`

- `test_otf_catalogs.py` / new: assert that a `TeradataRelation` built from a
  relation-config whose `alias` differs from `name` renders
  `"dl"."db"."<alias>"` (not `<name>`).
- Length-guardrail unit test (if G1 guardrail added): assert compiler error for
  an over-length alias.

### 5.2 Functional tests (gated on `DBT_TERADATA_DATALAKE` / `DBT_TERADATA_OTF_DATABASE`)

Add to `tests/functional/adapter/test_otf_integration.py` (and register alias
object names in the cleanup list — G7):

| Test | Maps to | Verifies |
|------|---------|----------|
| `test_otf_table_with_alias` | §1.1, G6 | aliased `table` materialization creates `"dl"."db"."<alias>"`; model-name object does **not** exist. |
| `test_otf_incremental_with_alias` | §1.2 | first run creates alias object; second run appends to it (row count grows); probe + staging + INSERT all hit the alias. |
| `test_ref_to_aliased_otf_model` | §1.3, G3 | downstream model `ref()`-ing the aliased model compiles/runs against the 3-part **alias** name. |
| `test_otf_alias_full_refresh` | §1.4, G6 | `--full-refresh` DROP+CREATE targets the alias. |
| `test_otf_long_alias` | G1, G2 | long alias either succeeds end-to-end or fails with the intended guardrail message; staging name stays valid. |

> The SIT PDF scenarios (HELP/SHOW ALIAS TABLE, time-travel via alias, DML
> rejection on alias, joins across alias tables) are tests for Teradata's
> ALIAS TABLE feature **(B)** and are **out of scope** here.

---

## 6. Compatibility check vs OTF limitations (from memory)

dbt `alias` only changes the **table name** dbt emits; it does not add DML or
new object types, so it inherits the existing OTF constraints without new
conflicts:

- **External OTF write limits** (no MERGE, copy-on-write only, positional
  INSERT, no multi-statement/rollback): unaffected — alias does not change DML.
  Incremental remains `append`-only (`incremental_otf.sql:27`).
- **OTF objects not in `DBC.TablesV` under the target schema**: still true for
  the aliased object; the incremental existence probe (`otf_relation_exists`,
  SAMPLE 0 + Error 7825 catch) already handles this and uses the alias name.
- **GRANTs unsupported on OTF**: unchanged; alias doesn't enable grants.
- **`CREATE ALIAS TABLE` (feature B)**: read-only, no DML, no ALTER — *not*
  what we generate. We must **not** accidentally emit `CREATE ALIAS TABLE`;
  the OTF path emits ordinary `CREATE TABLE … AS` with the alias as the name.
- **Identifier length (G1)**: the one genuine compatibility item to verify
  against Glue/Unity/Hive catalog limits before deciding on a guardrail.

---

## 7. Out of scope

- Teradata `CREATE/DROP ALIAS TABLE` DDL (feature B) and its SIT scenarios.
- Managed OTF (MOTF) aliasing.
- `on_schema_change`, `merge`/`delete+insert`/`valid_history` for OTF
  (tracked separately; still rejected by guardrails).
- Aliasing of OTF **sources** (alias is a model/seed/snapshot config).

---

## 8. Open questions / risks

1. **Re-scope confirmation:** IDE-26154's backlog title implies Teradata ALIAS
   TABLE (B); this design delivers dbt `alias` (A). Confirm with the team.
2. **Catalog identifier-length limits (G1):** need empirical confirmation per
   catalog before adding a guardrail vs. documenting only.
3. **Long-alias staging name (G2):** confirm `make_temp_relation` truncation
   keeps `<alias>__dbt_tmp` under the regular Teradata 128-char limit.
