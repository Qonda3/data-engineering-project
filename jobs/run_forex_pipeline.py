from utils.spark_session import get_spark
import yaml
from pipelines.forex import forex_ohlc_rollup, forex_indicators

def main():
    spark = get_spark('forex_pipeline')
    cfg = yaml.safe_load(open('config/forex_config.yaml'))
    forex_ohlc_rollup.run(spark, cfg['input_path'], {
        'm1': cfg['output_table_m1'],
        'm30': cfg['output_table_m30'],
        'h1': cfg['output_table_h1']
    })
    forex_indicators.run(spark, cfg['output_table_m1'], 'data/prepared/forex_m1_indicators')

if __name__ == '__main__':
    main()
