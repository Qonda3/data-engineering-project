from pyspark.sql import functions as F
from pyspark.sql.window import Window

ZAR_PER_SEC = 1.0 / 60.0
ZAR_PER_BYTE = 49.0 / 1_000_000_000.0

def run(spark, input_path: str, user_table: str, device_table: str, forex_table: str, output_table: str):
    usage = spark.read.format('parquet').load(input_path)
    usage = usage.withColumn('hour_start', F.date_trunc('hour', F.col('event_ts')))
    usage = usage.withColumn('cost_zar',
        F.when(F.col('cdr_type')=='voice', F.coalesce(F.col('duration_sec'),0)*ZAR_PER_SEC)
         .when(F.col('cdr_type')=='data', F.coalesce(F.col('bytes'),0)*ZAR_PER_BYTE)
         .otherwise(0.0))
    hourly = (usage.groupBy('msisdn','hour_start')
              .agg(F.sum('cost_zar').alias('hour_cost_zar'),
                   F.sum(F.when(F.col('cdr_type')=='voice', F.col('duration_sec')).otherwise(0)).alias('hour_voice_seconds'),
                   F.sum(F.when(F.col('cdr_type')=='data', F.col('bytes')).otherwise(0)).alias('hour_data_bytes')))
    hourly = hourly.join(spark.table(user_table), on='msisdn', how='left')
    hourly = hourly.join(spark.table(device_table), on='msisdn', how='left')
    hourly = hourly.join(spark.table(forex_table), hourly.hour_start==F.col('hour_start'), how='left')
    w = Window.partitionBy('msisdn').orderBy('hour_start').rowsBetween(Window.unboundedPreceding,0)
    hourly = hourly.withColumn('running_balance_zar', F.sum('hour_cost_zar').over(w))
    hourly.write.format('parquet').mode('overwrite').save(output_table)
