from utils.spark_session import get_spark
import yaml
from pipelines.cdr import cdr_summary_windowed, cdr_tower_sessions

def main():
    spark = get_spark('cdr_pipeline')
    cfg = yaml.safe_load(open('config/cdr_config.yaml'))
    input_path = cfg['input_path']
    sizes = cfg['window_sizes_seconds']
    outputs = {
        str(900): 'data/prepared/cdr_summary_15m',
        str(1800): 'data/prepared/cdr_summary_30m',
        str(3600): 'data/prepared/cdr_summary_1h'
    }
    cdr_summary_windowed.run(spark, input_path, sizes, outputs)
    cdr_tower_sessions.run(spark, input_path, 'data/prepared/cdr_tower_sessions', max_gap_seconds=1800)

if __name__ == '__main__':
    main()
