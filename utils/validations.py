from pyspark.sql import DataFrame

def assert_cols_exist(df: DataFrame, cols: list):
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise ValueError(f"Missing expected columns: {missing}")
