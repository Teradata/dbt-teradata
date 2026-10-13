# Scenario 7: model that returns an empty (schema-only) DataFrame.
import pandas as pd

def model(dbt, session):
    dbt.config(materialized="table")
    return pd.DataFrame({"id": pd.Series([], dtype="int64"), "name": pd.Series([], dtype="object")})
