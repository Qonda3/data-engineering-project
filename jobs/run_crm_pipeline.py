from utils.spark_session import get_spark
import yaml
from pipelines.crm import crm_hourly_summary

def main():
    spark = get_spark('crm_pipeline')
    cfg = yaml.safe_load(open('config/crm_config.yaml'))
    crm_hourly_summary.run(
        spark,
        input_path=cfg['input_path'],
        user_table='prepared.crm_users',
        device_table='prepared.crm_devices',
        forex_table='prepared.forex_hourly_rates',
        output_table=cfg['output_table_hourly']
    )

if __name__ == '__main__':
    main()
