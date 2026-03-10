from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType, LongType, MapType

cdr_schema = StructType([
    StructField('event_ts', TimestampType(), True),
    StructField('msisdn', StringType(), True),
    StructField('cdr_type', StringType(), True),
    StructField('call_type', StringType(), True),
    StructField('duration_sec', IntegerType(), True),
    StructField('bytes', LongType(), True),
    StructField('tower_id', StringType(), True),
    StructField('other_meta', MapType(StringType(), StringType()), True)
])
