# Scenario 6: use_fastload config flag exercised on write-back.
import pandas as pd

def model(dbt, session):
    dbt.config(materialized="table", use_fastload=True)
    return pd.DataFrame({"n": list(range(1, 51)), "sq": [i * i for i in range(1, 51)]})
