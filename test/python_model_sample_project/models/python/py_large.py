# Scenario 9: return more rows than the write-back batch size (2536) so the
# bulk INSERT is executed in multiple executemany batches. Demonstrates that
# batching loads every row correctly (here: 5000 rows).
import pandas as pd

def model(dbt, session):
    dbt.config(materialized="table")
    n = 5000
    return pd.DataFrame({"id": range(1, n + 1), "sq": [i * i for i in range(1, n + 1)]})
