Sample dbt project for client-side Python models (IDE-24196)
============================================================

Demonstrates every supported client-side Python-model scenario against a live
Teradata instance.

Layout
------
seeds/raw_orders.csv                     - source data (orders, 6 rows)
seeds/raw_customers.csv                  - source data (customers, 3 rows)
models/staging/stg_orders.sql            - SQL upstream model (orders)
models/staging/stg_customers.sql         - SQL upstream model (customers)
models/staging/sources.yml               - source() definition over the seed
models/python/
  py_scratch.py         - build a DataFrame from scratch (int/float/str/bool + nulls)
  py_ref_sql.py         - dbt.ref() a SQL model, pandas filter + computed column
  py_source.py          - dbt.source() a source table
  py_aggregate.py       - groupby aggregation over a python model (python->python)
  py_join.py            - a SINGLE python model joining MULTIPLE refs in pandas
  py_session_table.py   - uses the injected `session` object
  py_fastload.py        - use_fastload=True write path (50 rows)
  py_empty.py           - empty (schema-only) DataFrame
  py_types.py           - type mapping: datetime / timedelta / unicode / all-null
  py_large.py           - 5000 rows -> multi-batch insert (batch size 2536)
  py_teradataml.py      - OPTIONAL, disabled by default: return a teradataml
                          DataFrame (best-effort .to_pandas() write-back).
                          Requires teradataml; excluded from the default run.
models/downstream/
  customer_totals.sql   - SQL model consuming a python model (python->sql mixed DAG)
  schema.yml            - not_null / unique data tests

Run
---
Uses the `pdtest` profile from ~/.dbt/profiles.yml (Teradata, tmode=ANSI).

  cd test/python_model_sample_project
  dbt seed
  dbt run
  dbt test

Expected: dbt seed -> 2, dbt run -> 13 models PASS, dbt test -> 4 PASS.
(13 = 10 python + 3 SQL models. py_teradataml is disabled and does not run
unless selected explicitly.)

Re-run / atomicity (concern #4)
-------------------------------
Run `dbt run` a SECOND time to exercise the rebuild-in-place path, where each
target table already exists. Write-back is atomic: the new data is built under
a `<model>__dbt_tmp` staging table and only swapped into place once it is fully
loaded. So a re-run always leaves a valid table, and if a run is cancelled
(Ctrl-C) or a model fails mid-load, the PREVIOUS table is left intact rather
than dropped or half-populated. To see it, start a run and cancel it partway;
the existing tables remain queryable.

Notes
-----
- Python models run in the dbt runner process (pandas); intended for
  small/medium data. See the "Python models (Beta)" section in the
  repository's top-level README for details.
- Incremental python models are not supported in the MVP (table only).
- teradataml is accepted best-effort on return (via .to_pandas()); full
  first-class teradataml/pushdown support is the scalable Lake/OAF follow-on.
