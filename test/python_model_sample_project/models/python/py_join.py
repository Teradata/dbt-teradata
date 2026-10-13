# Scenario: a SINGLE python model referencing MULTIPLE tables.
# Each dbt.ref() is read as its own pandas DataFrame, then joined in pandas.
# (This is impossible with the SCRIPT Table Operator, which is single-table.)
def model(dbt, session):
    dbt.config(materialized="table")
    orders = dbt.ref("stg_orders")          # table 1
    customers = dbt.ref("stg_customers")    # table 2
    joined = orders.merge(customers, on="customer_id", how="inner")
    joined["amount_filled"] = joined["amount"].fillna(0.0)
    return joined[
        ["order_id", "customer_id", "customer_name", "region", "amount_filled", "status"]
    ]
