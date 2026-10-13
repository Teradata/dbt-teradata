# Scenario 8: exercise the pandas -> Teradata type mapping across dtypes that
# the other models don't cover — datetime, timedelta, unicode text, and an
# all-null object column. Demonstrates how each dtype lands as a column type.
import pandas as pd

def model(dbt, session):
    dbt.config(materialized="table")
    return pd.DataFrame(
        {
            "ts": pd.to_datetime(["2024-01-01 10:00:00", "2024-06-15 23:30:00"]),
            "span": pd.to_timedelta(["1 days", "2 days 03:04:05"]),
            "unicode_label": ["café", "naïve—text"],   # -> VARCHAR(...) UNICODE
            "all_null": pd.Series([None, None], dtype="object"),
        }
    )
