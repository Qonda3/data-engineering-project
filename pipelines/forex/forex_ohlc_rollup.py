from pyspark.sql import functions as F
from utils.time_windows import aligned_window_start

def rollup(df, window_seconds: int):
    ws = aligned_window_start(F.col('ts'), window_seconds)
    agg = (df.withColumn('window_start', ws)
           .groupBy('pair','window_start')
           .agg(
               F.first('price').alias('open'),
               F.max('price').alias('high'),
               F.min('price').alias('low'),
               F.last('price').alias('close'),
               F.sum('volume').alias('volume')
           ))
    return agg

def run(spark, input_path: str, outputs: dict):
    df = spark.read.format('parquet').load(input_path)
    m1 = rollup(df, 60)
    m30 = rollup(df, 1800)
    h1 = rollup(df, 3600)
    m1.write.format('parquet').mode('overwrite').save(outputs['m1'])
    m30.write.format('parquet').mode('overwrite').save(outputs['m30'])
    h1.write.format('parquet').mode('overwrite').save(outputs['h1'])
