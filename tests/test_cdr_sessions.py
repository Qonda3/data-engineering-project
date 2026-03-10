import pytest
from pyspark.sql import SparkSession, functions as F
from pipelines.cdr.cdr_tower_sessions import compute_sessions

def test_sessions():
    spark = SparkSession.builder.master('local[1]').appName('test').getOrCreate()
    data = [("2025-01-01 00:00:01","27100000001","T1"),
            ("2025-01-01 00:01:01","27100000001","T1"),
            ("2025-01-01 00:02:01","27100000001","T1"),
            ("2025-01-01 01:00:00","27100000001","T2")]
    df = spark.createDataFrame(data, schema=['event_ts','msisdn','tower_id']).withColumn('event_ts', F.col('event_ts').cast('timestamp'))
    sessions = compute_sessions(df).collect()
    assert len(sessions) == 1
