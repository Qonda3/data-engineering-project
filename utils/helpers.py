from pyspark.sql import functions as F

def coalesce_zero(col):
    return F.coalesce(col, F.lit(0))
