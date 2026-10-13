# Scenario 4: groupby aggregation over a python-model ref (python -> python chain).
def model(dbt, session):
    dbt.config(materialized="table")
    df = dbt.ref("py_ref_sql")
    agg = (
        df.groupby("customer_id", as_index=False)
        .agg(total_amount=("amount", "sum"), n_orders=("order_id", "count"))
    )
    return agg
