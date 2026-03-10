import pytest
from pyspark.sql import SparkSession, functions as F
from pipelines.cdr.cdr_summary_windowed import summarize_cdr

def test_window_aggregation():
    spark = SparkSession.builder.master('local[1]').appName('test').getOrCreate()
    data = [("2025-01-01 00:01:00","27100000001","voice","outgoing",30, None, None),
            ("2025-01-01 00:05:00","27100000001","data",None,None,1000,None)]
    df = spark.createDataFrame(data, schema=['event_ts','msisdn','cdr_type','call_type','duration_sec','bytes','tower_id'])
    df = df.withColumn('event_ts', F.col('event_ts').cast('timestamp'))
    out = summarize_cdr(df, 900).collect()
    assert len(out) == 1
