# Data Engineering Project 2024

See https://dev.curriculum.wethinkco.de/dataengineering/de-project/project-intro/ for instructions.






TBD 7 
# Prepared Layer Project

This project produces prepared-layer artifacts for CDR, CRM, and Forex data.

## How to run

1. Install dependencies (PySpark, pandas)
2. Place raw data in `data/raw/*`
3. Run pipelines:
    - `python jobs/run_cdr_pipeline.py`
    - `python jobs/run_crm_pipeline.py`
    - `python jobs/run_forex_pipeline.py`

Adjust configs in `config/*.yaml`.
