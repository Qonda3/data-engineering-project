#!/usr/bin/env python3
"""TBD 6: CRM Persister - FIXED VERSION"""

import json
import logging
import time
import sys
from datetime import datetime
from confluent_kafka import Consumer, KafkaError
import psycopg2

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

class CRMPersister:
    def __init__(self):
        logger.info("🚀 TBD 6: CRM Persister (Fixed)")
        
        self.crm_topics = [
            'wtc_prod_crm.crm_system.accounts',
            'wtc_prod_crm.crm_system.addresses',
            'wtc_prod_crm.crm_system.devices'
        ]
        
        self.kafka_brokers = 'redpanda-0:9092'
        self.processed_count = 0
        
    def setup_postgres(self):
        """Setup PostgreSQL with CORRECT schema"""
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
            
            # Create crm_data schema
            self.pg_cursor.execute("CREATE SCHEMA IF NOT EXISTS crm_data")
            
            # Drop and recreate tables with CORRECT schema
            self.create_correct_tables()
            self.pg_conn.commit()
            logger.info("📊 Created CORRECT crm_data tables")
            
        except Exception as e:
            logger.error(f"❌ PostgreSQL error: {e}")
            raise
    
    def create_correct_tables(self):
        """Create tables matching ACTUAL CRM schema"""
        # Drop old tables if they exist
        for table in ['accounts', 'addresses', 'devices']:
            self.pg_cursor.execute(f"DROP TABLE IF EXISTS crm_data.{table} CASCADE")
        
        # Accounts table - MATCHING ACTUAL FIELDS
        self.pg_cursor.execute("""
            CREATE TABLE crm_data.accounts (
                account_id INTEGER PRIMARY KEY,
                owner_name VARCHAR(255),
                email VARCHAR(255),
                phone_number VARCHAR(50),
                modified_ts BIGINT,
                cdc_operation CHAR(1),
                cdc_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Addresses table - MATCHING ACTUAL FIELDS
        self.pg_cursor.execute("""
            CREATE TABLE crm_data.addresses (
                account_id INTEGER PRIMARY KEY,
                street_address VARCHAR(255),
                city VARCHAR(100),
                state VARCHAR(50),
                postal_code VARCHAR(20),
                country VARCHAR(100),
                modified_ts BIGINT,
                cdc_operation CHAR(1),
                cdc_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Devices table (need to check actual fields from sample)
        self.pg_cursor.execute("""
            CREATE TABLE crm_data.devices (
                device_id SERIAL PRIMARY KEY,
                account_id INTEGER,
                device_type VARCHAR(50),
                device_model VARCHAR(100),
                imei VARCHAR(50),
                activated_at TIMESTAMP,
                modified_ts BIGINT,
                cdc_operation CHAR(1),
                cdc_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        logger.info("✅ Created tables with ACTUAL field names")
    
    def setup_kafka(self):
        """Setup Kafka consumer"""
        try:
            self.consumer = Consumer({
                'bootstrap.servers': self.kafka_brokers,
                'group.id': 'tbd6-crm-persister-fixed',
                'auto.offset.reset': 'earliest',
                'enable.auto.commit': True
            })
            self.consumer.subscribe(self.crm_topics)
            logger.info(f"✅ Connected to Redpanda")
            logger.info(f"📨 Subscribed to: {self.crm_topics}")
            
        except Exception as e:
            logger.error(f"❌ Kafka error: {e}")
            raise
    
    def process_message(self, msg):
        """Process Debezium CDC message CORRECTLY"""
        try:
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    return
                else:
                    logger.error(f"Kafka error: {msg.error()}")
                    return
            
            # Parse the COMPLEX Debezium message
            msg_data = json.loads(msg.value().decode('utf-8'))
            self.processed_count += 1
            
            # Extract payload (the actual data)
            payload = msg_data.get('payload', {})
            operation = payload.get('op', '')
            after_data = payload.get('after', {})
            source = payload.get('source', {})
            table = source.get('table', '')
            
            # Process based on table
            if table == 'accounts':
                self.insert_account(after_data, operation)
            elif table == 'addresses':
                self.insert_address(after_data, operation)
            elif table == 'devices':
                self.insert_device(after_data, operation)
            else:
                logger.warning(f"Unknown table: {table}")
                return
            
            # Log progress
            if self.processed_count % 100 == 0:
                logger.info(f"💾 Persisted {self.processed_count} CRM records")
                
        except Exception as e:
            logger.error(f"❌ Processing error: {e}")
            logger.error(f"Message data: {json.dumps(msg_data, indent=2)[:500]}")
            self.pg_conn.rollback()
    
    def insert_account(self, data, operation):
        """Insert account data with ACTUAL fields"""
        insert_sql = """
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
        """
        
        self.pg_cursor.execute(insert_sql, (
            data.get('account_id'),
            data.get('owner_name'),
            data.get('email'),
            data.get('phone_number'),
            data.get('modified_ts'),
            operation
        ))
        self.pg_conn.commit()
    
    def insert_address(self, data, operation):
        """Insert address data with ACTUAL fields"""
        insert_sql = """
            INSERT INTO crm_data.addresses 
            (account_id, street_address, city, state, postal_code, country, modified_ts, cdc_operation)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (account_id) DO UPDATE SET
                street_address = EXCLUDED.street_address,
                city = EXCLUDED.city,
                state = EXCLUDED.state,
                postal_code = EXCLUDED.postal_code,
                country = EXCLUDED.country,
                modified_ts = EXCLUDED.modified_ts,
                cdc_operation = EXCLUDED.cdc_operation,
                cdc_timestamp = CURRENT_TIMESTAMP
        """
        
        self.pg_cursor.execute(insert_sql, (
            data.get('account_id'),
            data.get('street_address'),
            data.get('city'),
            data.get('state'),
            data.get('postal_code'),
            data.get('country'),
            data.get('modified_ts'),
            operation
        ))
        self.pg_conn.commit()
    
    def insert_device(self, data, operation):
        """Insert device data (need to check actual fields)"""
        # For now, log what we get
        logger.info(f"Device data sample: {list(data.keys())}")
        # TODO: Implement when we see actual device data
    
    def run(self):
        """Main processing loop"""
        logger.info("🎬 Starting FIXED CRM Persister")
        
        try:
            self.setup_postgres()
            self.setup_kafka()
            
            logger.info("=" * 50)
            logger.info("✅ TBD 6: CRM Persister FIXED and READY!")
            logger.info("   Processing COMPLEX Debezium CDC format")
            logger.info("   Target: PostgreSQL 'crm_data' (correct schema)")
            logger.info("=" * 50)
            
            # Main loop
            start_time = time.time()
            while True:
                msg = self.consumer.poll(1.0)
                
                if msg:
                    self.process_message(msg)
                
                # Status updates
                elapsed = time.time() - start_time
                if elapsed > 30 and self.processed_count == 0:
                    logger.info("⏳ Waiting for CRM CDC data...")
                
                time.sleep(0.1)
                
        except Exception as e:
            logger.error(f"💥 Fatal error: {e}")
            raise
    
    def shutdown(self):
        """Clean shutdown"""
        logger.info(f"🛑 Shutdown. Total CRM records: {self.processed_count}")
        if hasattr(self, 'consumer'):
            self.consumer.close()
        if hasattr(self, 'pg_cursor'):
            self.pg_cursor.close()
        if hasattr(self, 'pg_conn'):
            self.pg_conn.close()

def main():
    persister = CRMPersister()
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
