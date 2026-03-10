#!/usr/bin/env python3
"""TBD 6: CDR Persister - Waiting for TBD 1 Data"""

import logging
import time
import sys
from confluent_kafka import Consumer

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def main():
    logger.info("🚀 TBD 6: CDR Persister Starting")
    logger.info("⏳ Waiting for TBD 1 to populate cdr_data/cdr_voice topics...")
    
    consumer = Consumer({
        'bootstrap.servers': 'redpanda-0:9092',
        'group.id': 'tbd6-cdr-persister-waiting',
        'auto.offset.reset': 'earliest'
    })
    consumer.subscribe(['cdr_data', 'cdr_voice'])
    
    count = 0
    wait_counter = 0
    
    try:
        while True:
            msg = consumer.poll(1.0)
            
            if msg and not msg.error():
                count += 1
                logger.info(f"🎉 FIRST CDR MESSAGE RECEIVED! Total: {count}")
                logger.info("✅ CDR persistence is now active!")
            
            # Status update every 30 seconds
            wait_counter += 1
            if wait_counter % 30 == 0 and count == 0:
                logger.info("⏳ Still waiting for TBD 1 data in cdr_data/cdr_voice...")
            
            time.sleep(1)
    finally:
        consumer.close()

if __name__ == "__main__":
    main()
