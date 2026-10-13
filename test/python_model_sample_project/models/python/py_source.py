# Scenario 3: read from a dbt source().
def model(dbt, session):
    dbt.config(materialized="table")
    df = dbt.source("raw", "raw_orders")
    return df[["order_id", "customer_id", "amount"]]
