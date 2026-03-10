from pyspark.sql import functions as F
from utils.time_windows import aligned_window_start

def summarize_cdr(df, window_seconds: int):
    ws = aligned_window_start(F.col('event_ts'), window_seconds)
    grouped = (df
               .withColumn('window_start', ws)
               .groupBy('window_start', 'msisdn')
               .agg(
                   F.sum(F.when(F.col('cdr_type')=='voice',1).otherwise(0)).alias('voice_count'),
                   F.sum(F.when(F.col('cdr_type')=='voice',F.coalesce(F.col('duration_sec'),F.lit(0))).otherwise(0)).alias('voice_seconds'),
                   F.sum(F.when(F.col('cdr_type')=='data',F.coalesce(F.col('bytes'),F.lit(0))).otherwise(0)).alias('data_bytes'),
                   F.count('*').alias('total_records')
               ))
    return grouped

def run(spark, input_path: str, window_sizes: list, output_tables: dict):
    df = spark.read.format('parquet').load(input_path)
    for ws in window_sizes:
        out_table = output_tables.get(str(ws))
        summary = summarize_cdr(df, ws)
        summary.write.format('parquet').mode('overwrite').save(out_table)
