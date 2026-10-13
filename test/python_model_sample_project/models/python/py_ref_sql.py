# Scenario 2: ref a SQL model, filter + compute with pandas.
def model(dbt, session):
    dbt.config(materialized="table")
    df = dbt.ref("stg_orders")
    df = df[df["status"] == "completed"].copy()
    df["amount_with_tax"] = df["amount"] * 1.1
    return df
