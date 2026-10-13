# Scenario 1: build a DataFrame from scratch (no ref), multiple dtypes + nulls.
import pandas as pd
import numpy as np

def model(dbt, session):
    dbt.config(materialized="table")
    return pd.DataFrame(
        {
            "id": [1, 2, 3, 4],
            "score": [1.5, 2.5, np.nan, 4.0],
            "label": ["a", "b", None, "dddd"],
            "flag": [True, False, True, False],
        }
    )
