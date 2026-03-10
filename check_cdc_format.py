#!/usr/bin/env python3
import json

# Sample Debezium CDC format
sample_cdc = {
    "op": "c",  # operation: c=create, u=update, d=delete, r=read
    "after": {
        "id": 1,
        "customer_name": "John Doe",
        "account_type": "premium",
        "balance": 1000.50,
        "created_at": "2024-01-01T00:00:00Z",
        "updated_at": "2024-01-01T00:00:00Z"
    },
    "source": {
        "table": "accounts",
        "db": "wtc_prod",
        "schema": "crm_system"
    },
    "ts_ms": 1704067200000
}

print("Expected Debezium CDC format:")
print(json.dumps(sample_cdc, indent=2))
