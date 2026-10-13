# Scenario 5: the injected `session` object is available as the 2nd argument.
# It is a thin shim (session.table(name) also returns pandas); here we read via
# dbt.ref and use session only to show it is passed in.
def model(dbt, session):
    dbt.config(materialized="table")
    df = dbt.ref("stg_orders")
    _ = session            # available as the 2nd arg (thin shim); not needed here
    df = df.copy()
    df["is_completed"] = (df["status"] == "completed").astype(int)
    return df
