#!/usr/bin/env python3
"""Debug accounts insertion"""

import json
import logging
import time
import sys
from confluent_kafka import Consumer
import psycopg2

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def main():
    logger.info("🔍 Debugging Accounts Insertion")
    
    # Setup
    conn = psycopg2.connect(
        host='postgres', port='5432',
        database='wtc_analytics',
        user='postgres', password='postgres'
    )
    cursor = conn.cursor()
    
    consumer = Consumer({
        'bootstrap.servers': 'redpanda-0:9092',
        'group.id': 'debug-accounts',
        'auto.offset.reset': 'earliest'
    })
    consumer.subscribe(['wtc_prod_crm.crm_system.accounts'])
    
    count = 0
    try:
        while count < 5:  # Just process 5 messages
            msg = consumer.poll(1.0)
            if msg and not msg.error():
                count += 1
                data = json.loads(msg.value().decode('utf-8'))
                payload = data.get('payload', {})
                after = payload.get('after', {})
                
                logger.info(f"\n=== Message {count} ===")
                logger.info(f"Operation: {payload.get('op')}")
                logger.info(f"After data: {after}")
                
                # Try to insert
                try:
                    cursor.execute("""
                        INSERT INTO crm_data.accounts 
                        (account_id, owner_name, email, phone_number, modified_ts, cdc_operation)
                        VALUES (%s, %s, %s, %s, %s, %s)
                        ON CONFLICT (account_id) DO UPDATE SET
                            owner_name = EXCLUDED.owner_name,
                            email = EXCLUDED.email,
                            phone_number = EXCLUDED.phone_number,
                            modified_ts = EXCLUDED.modified_ts,
                            cdc_operation = EXCLUDED.cdc_operation,
                            cdc_timestamp = CURRENT_TIMESTAMP
                    """, (
                        after.get('account_id'),
                        after.get('owner_name'),
                        after.get('email'),
                        after.get('phone_number'),
                        after.get('modified_ts'),
                        payload.get('op')
                    ))
                    conn.commit()
                    logger.info("✅ INSERT SUCCESSFUL")
                    
                    # Verify
                    cursor.execute("SELECT COUNT(*) FROM crm_data.accounts WHERE account_id = %s", 
                                  (after.get('account_id'),))
                    verify = cursor.fetchone()[0]
                    logger.info(f"✅ Verification: {verify} records with account_id={after.get('account_id')}")
                    
                except Exception as e:
                    logger.error(f"❌ INSERT FAILED: {e}")
                    conn.rollback()
            
            time.sleep(1)
    finally:
        cursor.close()
        conn.close()
        consumer.close()
        
    logger.info(f"\n📊 Processed {count} accounts messages")

if __name__ == "__main__":
    main()
