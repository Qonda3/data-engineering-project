#!/usr/bin/env python3
"""TBD 6: Forex Data Persistence Service"""

import json
import logging
import time
import sys
from datetime import datetime
from confluent_kafka import Consumer, KafkaError
import psycopg2

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

class ForexPersister:
    def __init__(self):
        logger.info("🚀 TBD 6: Forex Persistence Service")
        
        # Configuration
        self.kafka_brokers = 'redpanda-0:9092'
        self.processed_count = 0
        
    def connect_postgres(self):
        """Connect to PostgreSQL and setup schema"""
        try:
            self.pg_conn = psycopg2.connect(
                host='postgres',
                port='5432',
                database='wtc_analytics',
                user='postgres',
                password='postgres'
            )
            self.pg_cursor = self.pg_conn.cursor()
            logger.info("✅ Connected to PostgreSQL")
            
            # Create forex_data schema and table
            self.pg_cursor.execute("CREATE SCHEMA IF NOT EXISTS forex_data")
            
            create_table_sql = """
            CREATE TABLE IF NOT EXISTS forex_data.tick_data (
                id SERIAL PRIMARY KEY,
                timestamp TIMESTAMP NOT NULL,
                pair_name VARCHAR(10) NOT NULL,
                bid_price DECIMAL(10, 6),
                ask_price DECIMAL(10, 6),
                spread DECIMAL(10, 6),
                inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            CREATE INDEX IF NOT EXISTS idx_forex_pair_time ON forex_data.tick_data(pair_name, timestamp);
            """
            self.pg_cursor.execute(create_table_sql)
            self.pg_conn.commit()
            logger.info("📊 Created forex_data.tick_data table")
            
        except Exception as e:
            logger.error(f"❌ PostgreSQL error: {e}")
            raise
    
    def connect_redpanda(self):
        """Connect to Redpanda/Kafka"""
        try:
            self.consumer = Consumer({
                'bootstrap.servers': self.kafka_brokers,
                'group.id': 'tbd6-forex-persister',
                'auto.offset.reset': 'earliest',
                'enable.auto.commit': True
            })
            self.consumer.subscribe(['tick-data'])
            logger.info(f"✅ Connected to Redpanda: {self.kafka_brokers}")
            
        except Exception as e:
            logger.error(f"❌ Redpanda error: {e}")
            raise
    
    def process_tick(self, msg):
        """Process a forex tick message"""
        try:
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    return
                else:
                    logger.error(f"Kafka error: {msg.error()}")
                    return
            
            # Parse the message (nested JSON)
            msg_data = json.loads(msg.value().decode('utf-8'))
            self.processed_count += 1
            
            # Extract data
            tick = {
                'timestamp': msg_data['timestamp'],
                'pair_name': msg_data['pair_name'],
                'bid_price': float(msg_data['bid_price']),
                'ask_price': float(msg_data['ask_price']),
                'spread': float(msg_data['spread'])
            }
            
            # Parse timestamp
            try:
                timestamp = datetime.strptime(tick['timestamp'], '%Y-%m-%d %H:%M:%S.%f')
            except:
                timestamp = datetime.fromisoformat(tick['timestamp'].replace('Z', ''))
            
            # Insert into PostgreSQL
            insert_sql = """
                INSERT INTO forex_data.tick_data 
                (timestamp, pair_name, bid_price, ask_price, spread)
                VALUES (%s, %s, %s, %s, %s)
            """
            
            self.pg_cursor.execute(insert_sql, (
                timestamp,
                tick['pair_name'],
                tick['bid_price'],
                tick['ask_price'],
                tick['spread']
            ))
            self.pg_conn.commit()
            
            # Log progress
            if self.processed_count % 100 == 0:
                logger.info(f"💾 Persisted {self.processed_count} forex ticks")
                
        except Exception as e:
            logger.error(f"❌ Processing error: {e}")
            self.pg_conn.rollback()
    
    def run(self):
        """Main processing loop"""
        logger.info("🎬 Starting Forex Persistence Service")
        
        try:
            self.connect_postgres()
            self.connect_redpanda()
            
            logger.info("=" * 50)
            logger.info("✅ TBD 6: Forex Persister READY!")
            logger.info("   Source: Redpanda 'tick-data' topic")
            logger.info("   Target: PostgreSQL 'forex_data.tick_data'")
            logger.info("=" * 50)
            
            # Main loop
            start_time = time.time()
            while True:
                msg = self.consumer.poll(1.0)
                
                if msg:
                    self.process_tick(msg)
                
                # Status updates
                elapsed = time.time() - start_time
                if elapsed > 30 and self.processed_count == 0:
                    logger.info("⏳ Waiting for forex data...")
                
                time.sleep(0.1)
                
        except Exception as e:
            logger.error(f"💥 Fatal error: {e}")
            raise
    
    def shutdown(self):
        """Clean shutdown"""
        logger.info(f"🛑 Shutting down. Total ticks persisted: {self.processed_count}")
        if hasattr(self, 'consumer'):
            self.consumer.close()
        if hasattr(self, 'pg_cursor'):
            self.pg_cursor.close()
        if hasattr(self, 'pg_conn'):
            self.pg_conn.close()

def main():
    persister = ForexPersister()
    try:
        persister.run()
    except KeyboardInterrupt:
        logger.info("👋 Interrupted by user")
    except Exception as e:
        logger.error(f"💥 Fatal error: {e}")
        sys.exit(1)
    finally:
        persister.shutdown()

if __name__ == "__main__":
    main()
