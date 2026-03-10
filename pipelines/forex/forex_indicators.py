from utils.indicators import compute_indicators

def run(spark, input_table: str, output_table: str):
    df = spark.read.format('parquet').load(input_table)
    # compute indicators grouped by pair
    df_with_ind = df.groupBy('pair').apply(compute_indicators)
    df_with_ind.write.format('parquet').mode('overwrite').save(output_table)
