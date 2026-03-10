import pandas as pd
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DoubleType

schema = StructType([
    StructField("pair", StringType()),
    StructField("window_start", TimestampType()),
    StructField("open", DoubleType()),
    StructField("high", DoubleType()),
    StructField("low", DoubleType()),
    StructField("close", DoubleType()),
    StructField("volume", DoubleType()),
    StructField("ema8", DoubleType()),
    StructField("ema21", DoubleType()),
    StructField("atr8", DoubleType()),
    StructField("atr21", DoubleType())
])

@pandas_udf(schema)
def compute_indicators(pdf):
    pdf = pdf.sort_values("window_start").reset_index(drop=True)
    open_prices = pdf['open'].astype(float)
    ema8 = open_prices.ewm(span=8, adjust=False).mean()
    ema21 = open_prices.ewm(span=21, adjust=False).mean()
    prev_close = pdf['close'].shift(1)
    tr = pd.concat([
        pdf['high'] - pdf['low'],
        (pdf['high'] - prev_close).abs(),
        (pdf['low'] - prev_close).abs()
    ], axis=1).max(axis=1)
    atr8 = tr.rolling(8, min_periods=1).mean()
    atr21 = tr.rolling(21, min_periods=1).mean()
    pdf['ema8'] = ema8
    pdf['ema21'] = ema21
    pdf['atr8'] = atr8
    pdf['atr21'] = atr21
    return pdf[['pair','window_start','open','high','low','close','volume','ema8','ema21','atr8','atr21']]
