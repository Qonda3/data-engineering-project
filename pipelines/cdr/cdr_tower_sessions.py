from pyspark.sql import functions as F
from pyspark.sql.window import Window

def compute_sessions(df, max_gap_seconds: int = None):
    w = Window.partitionBy('msisdn').orderBy('event_ts')
    df2 = (df.withColumn('prev_tower', F.lag('tower_id').over(w))
             .withColumn('prev_ts', F.lag('event_ts').over(w))
             .withColumn('is_new_run', F.when(F.col('tower_id') != F.col('prev_tower'),1).otherwise(0))
             .withColumn('is_new_run', F.when(F.col('prev_tower').isNull(),1).otherwise(F.col('is_new_run'))))
    if max_gap_seconds is not None:
        gap = F.when((F.col('event_ts').cast('long') - F.col('prev_ts').cast('long')) > max_gap_seconds, 1).otherwise(0)
        df2 = df2.withColumn('is_new_run', F.when(gap==1,1).otherwise(F.col('is_new_run')))
    df2 = df2.withColumn('run_id', F.sum('is_new_run').over(w))
    sessions = (df2.groupBy('msisdn','run_id','tower_id')
                .agg(F.count('*').alias('interactions'),
                     F.min('event_ts').alias('session_start'),
                     F.max('event_ts').alias('session_end'))
                .filter(F.col('interactions') >= 3))
    return sessions

def run(spark, input_path: str, output_table: str, max_gap_seconds: int = None):
    df = spark.read.format('parquet').load(input_path).select('msisdn','tower_id','event_ts')
    sessions = compute_sessions(df, max_gap_seconds)
    sessions.write.format('parquet').mode('overwrite').save(output_table)
