from pipelines.crm.crm_hourly_summary import ZAR_PER_SEC, ZAR_PER_BYTE

def test_costs():
    assert ZAR_PER_SEC == 1.0 / 60.0
    assert ZAR_PER_BYTE == 49.0 / 1_000_000_000.0
