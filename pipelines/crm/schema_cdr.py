from pyspark.sql.types import StructType, StructField, StringType, TimestampType, LongType, DoubleType

crm_usage_schema = StructType([
    StructField('event_ts', TimestampType(), True),
    StructField('msisdn', StringType(), True),
    StructField('cdr_type', StringType(), True),
    StructField('duration_sec', LongType(), True),
    StructField('bytes', LongType(), True),
    StructField('unit_price_zar', DoubleType(), True)
])
