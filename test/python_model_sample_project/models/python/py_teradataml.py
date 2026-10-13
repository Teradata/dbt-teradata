# Scenario (OPTIONAL / disabled by default): return a teradataml DataFrame.
#
# The client-side write-back accepts a teradataml DataFrame on return as a
# best-effort convenience: if the returned object exposes .to_pandas() it is
# converted to pandas and written back like any other model. This demonstrates
# concern #2 (leveraging teradataml) at the MVP level. Full first-class
# teradataml / pushdown support is the scalable Lake/OAF follow-on track.
#
# This model is DISABLED (enabled=False) so it is NOT part of the default
# `dbt run` and does not affect the "everything PASS" expectation: it requires
# the `teradataml` package to be installed in the dbt runner's environment,
# which the other scenarios deliberately do not. To try it, install teradataml
# in the runner and run explicitly:
#
#     pip install teradataml
#     dbt run --select py_teradataml
#
def model(dbt, session):
    dbt.config(materialized="table", enabled=False)

    # teradataml is imported lazily so parsing this file never requires it.
    from teradataml import DataFrame

    # Build a teradataml DataFrame over an upstream relation and return it.
    # The adapter detects .to_pandas() and writes the result back normally.
    tdf = DataFrame.from_query("SELECT order_id, customer_id, amount FROM stg_orders")
    return tdf
