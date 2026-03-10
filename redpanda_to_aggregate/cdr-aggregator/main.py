#!/usr/bin/env python3
"""CDR Aggregator - TBD 3 - True Streaming with Error Recovery"""

import json
import logging
import time
import os
from datetime import datetime
from confluent_kafka import Consumer, KafkaError
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import sys

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

class CDRAggregator:
    def __init__(self):
        logger.info("Initializing CDR Aggregator...")
        
        # Configuration from environment variables with defaults
        self.kafka_brokers = os.getenv('KAFKA_BROKERS', 'localhost:19092')
        self.scylla_host = os.getenv('SCYLLA_HOST', 'scylladb')
        self.scylla_port = int(os.getenv('SCYLLA_PORT', '9042'))
        self.scylla_user = os.getenv('SCYLLA_USER', 'cassandra')
        self.scylla_pass = os.getenv('SCYLLA_PASS', 'cassandra')
        self.max_retries = int(os.getenv('MAX_RETRIES', '3'))
        self.retry_delay = float(os.getenv('RETRY_DELAY', '1.0'))
        
        # Billing rates
        self.voice_rate_zar_per_min = 1.0      # 1 ZAR/minute, billed per second
        self.data_rate_zar_per_gb = 49.0       # 49 ZAR/GB, billed per byte
        
        # Track processed offsets for idempotency
        self.processed_offsets = {}  # {topic: {partition: offset}}
        
        logger.info(f"Config - Kafka: {self.kafka_brokers}")
        logger.info(f"Config - ScyllaDB: {self.scylla_host}:{self.scylla_port}")
        logger.info(f"Config - Max retries: {self.max_retries}, Retry delay: {self.retry_delay}s")
        
        # Initialize connections
        self.setup_kafka()
        self.setup_scylla()
        
        # Counters
        self.message_count = 0
        self.data_records_processed = 0
        self.voice_records_processed = 0
        self.scylla_writes = 0
        self.write_failures = 0
        self.duplicates_skipped = 0
        
    def setup_kafka(self):
        """Setup Kafka consumer"""
        try:
            self.consumer = Consumer({
                'bootstrap.servers': self.kafka_brokers,
                'group.id': 'cdr-aggregator-group',
                'auto.offset.reset': 'earliest',
                'enable.auto.commit': False,  # Manual commit for idempotency
                'max.poll.interval.ms': 300000,
                'session.timeout.ms': 30000
            })
            
            # Subscribe to CDR topics
            self.consumer.subscribe(['cdr_data', 'cdr_voice'])
            logger.info(f"Connected to Kafka at {self.kafka_brokers}")
            
        except Exception as e:
            logger.error(f"Failed to connect to Kafka: {e}")
            raise
    
    def setup_scylla(self):
        """Setup ScyllaDB connection"""
        try:
            auth_provider = PlainTextAuthProvider(
                username=self.scylla_user,
                password=self.scylla_pass
            )
            
            self.cluster = Cluster(
                [self.scylla_host],
                auth_provider=auth_provider,
                port=self.scylla_port,
                connect_timeout=10
            )
            
            self.session = self.cluster.connect()
            
            # Create schema
            self.create_schema()
            logger.info(f"Connected to ScyllaDB at {self.scylla_host}:{self.scylla_port}")
            
        except Exception as e:
            logger.error(f"Failed to connect to ScyllaDB: {e}")
            raise
    
    def create_schema(self):
        """Create keyspace and table if they don't exist"""
        # Create keyspace
        keyspace_query = """
        CREATE KEYSPACE IF NOT EXISTS cdr_analytics
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}
        """
        
        try:
            self.session.execute(keyspace_query)
            self.session.set_keyspace('cdr_analytics')
            logger.info("Using keyspace: cdr_analytics")
        except Exception as e:
            logger.error(f"Error with keyspace: {e}")
            try:
                self.session.execute("USE cdr_analytics")
            except Exception as e2:
                logger.error(f"Failed to use keyspace: {e2}")
                raise
        
        # Create table with idempotency key
        table_query = """
        CREATE TABLE IF NOT EXISTS cdr_daily_summary (
            msisdn text,
            date text,
            usage_type text,
            total_up_bytes bigint,
            total_down_bytes bigint,
            total_call_seconds bigint,
            data_cost decimal,
            voice_cost decimal,
            last_updated timestamp,
            PRIMARY KEY ((msisdn, date), usage_type)
        )
        """
        
        try:
            self.session.execute(table_query)
            logger.info("Table cdr_daily_summary created/verified")
        except Exception as e:
            logger.error(f"Error creating table: {e}")
    
    def calculate_data_cost(self, up_bytes, down_bytes):
        """Calculate data cost: 49 ZAR/GB, billed per byte"""
        total_bytes = up_bytes + down_bytes
        total_gb = total_bytes / (1024 ** 3)
        return total_gb * self.data_rate_zar_per_gb
    
    def calculate_voice_cost(self, duration_sec):
        """Calculate voice cost: 1 ZAR/minute, billed per second"""
        duration_min = duration_sec / 60.0
        return duration_min * self.voice_rate_zar_per_min
    
    def write_to_scylla(self, msisdn, date, usage_type, up_bytes=0, down_bytes=0, 
                        call_seconds=0, data_cost=0.0, voice_cost=0.0):
        """Write to ScyllaDB with retry logic"""
        retry_count = 0
        
        while retry_count < self.max_retries:
            try:
                query = """
                INSERT INTO cdr_daily_summary 
                (msisdn, date, usage_type, total_up_bytes, total_down_bytes, 
                 total_call_seconds, data_cost, voice_cost, last_updated)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                
                self.session.execute(query, (
                    msisdn,
                    date,
                    usage_type,
                    int(up_bytes),
                    int(down_bytes),
                    int(call_seconds),
                    float(data_cost),
                    float(voice_cost),
                    datetime.now()
                ))
                
                self.scylla_writes += 1
                logger.debug(f"Written to ScyllaDB: {msisdn} | {date} | {usage_type}")
                return True
                
            except Exception as e:
                retry_count += 1
                if retry_count < self.max_retries:
                    logger.warning(
                        f"ScyllaDB write failed (retry {retry_count}/{self.max_retries}): {e}"
                    )
                    time.sleep(self.retry_delay)
                else:
                    logger.error(
                        f"ScyllaDB write failed after {self.max_retries} retries: {e}"
                    )
                    self.write_failures += 1
                    return False
    
    def is_duplicate(self, topic, partition, offset):
        """Check if message has already been processed (idempotency)"""
        if topic not in self.processed_offsets:
            self.processed_offsets[topic] = {}
        
        current_offset = self.processed_offsets[topic].get(partition, -1)
        
        if offset <= current_offset:
            logger.warning(f"Duplicate detected: {topic}[{partition}] offset {offset}")
            self.duplicates_skipped += 1
            return True
        
        # Update tracked offset
        self.processed_offsets[topic][partition] = offset
        return False
    
    def commit_offset(self, msg):
        """Manually commit offset after successful processing"""
        try:
            self.consumer.commit(msg, asynchronous=False)
        except Exception as e:
            logger.warning(f"Failed to commit offset: {e}")
    
    def process_cdr_data(self, data):
        """Process CDR data message (bytes up/down)"""
        try:
            msisdn = data.get('msisdn')
            event_datetime = data.get('event_datetime')
            data_type = data.get('data_type', 'unknown')
            up_bytes = int(data.get('up_bytes', 0))
            down_bytes = int(data.get('down_bytes', 0))
            
            if not msisdn or not event_datetime:
                logger.warning("Missing required fields in CDR data")
                return False
            
            # Extract date from datetime
            event_date = datetime.fromisoformat(event_datetime).date()
            
            # Calculate data cost
            data_cost = self.calculate_data_cost(up_bytes, down_bytes)
            
            logger.info(
                f"CDR Data | MSISDN: {msisdn} | Date: {event_date} | Type: {data_type} | "
                f"Up: {up_bytes} bytes | Down: {down_bytes} bytes | Cost: {data_cost:.4f} ZAR"
            )
            
            # Write to ScyllaDB
            success = self.write_to_scylla(
                msisdn=msisdn,
                date=str(event_date),
                usage_type=data_type,
                up_bytes=up_bytes,
                down_bytes=down_bytes,
                data_cost=data_cost
            )
            
            if success:
                self.data_records_processed += 1
            
            return success
            
        except Exception as e:
            logger.error(f"Error processing CDR data: {e}")
            return False
    
    def process_cdr_voice(self, data):
        """Process CDR voice message (call duration, cost)"""
        try:
            msisdn = data.get('msisdn')
            start_time = data.get('start_time')
            call_type = data.get('call_type', 'voice')
            call_duration_sec = int(data.get('call_duration_sec', 0))
            
            if not msisdn or not start_time:
                logger.warning("Missing required fields in CDR voice")
                return False
            
            # Extract date from start_time
            start_date = datetime.fromisoformat(start_time).date()
            
            # Calculate voice cost
            voice_cost = self.calculate_voice_cost(call_duration_sec)
            
            logger.info(
                f"CDR Voice | MSISDN: {msisdn} | Date: {start_date} | Type: {call_type} | "
                f"Duration: {call_duration_sec} sec | Cost: {voice_cost:.4f} ZAR"
            )
            
            # Write to ScyllaDB
            success = self.write_to_scylla(
                msisdn=msisdn,
                date=str(start_date),
                usage_type=call_type,
                call_seconds=call_duration_sec,
                voice_cost=voice_cost
            )
            
            if success:
                self.voice_records_processed += 1
            
            return success
            
        except Exception as e:
            logger.error(f"Error processing CDR voice: {e}")
            return False
    
    def process_message(self, msg):
        """Process a Kafka message"""
        try:
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    logger.debug("End of partition reached")
                else:
                    logger.error(f"Kafka error: {msg.error()}")
                return False
            
            # Check for duplicates (idempotency)
            if self.is_duplicate(msg.topic(), msg.partition(), msg.offset()):
                self.commit_offset(msg)
                return True
            
            # Parse message
            data = json.loads(msg.value().decode('utf-8'))
            self.message_count += 1
            
            topic = msg.topic()
            success = False
            
            # Route to appropriate processor
            if topic == 'cdr_data':
                success = self.process_cdr_data(data)
            elif topic == 'cdr_voice':
                success = self.process_cdr_voice(data)
            else:
                logger.warning(f"Unknown topic: {topic}")
            
            # Commit offset only after successful processing
            if success:
                self.commit_offset(msg)
            
            return success
            
        except json.JSONDecodeError as e:
            logger.error(f"Error decoding JSON message: {e}")
            return False
        except Exception as e:
            logger.error(f"Error processing message: {e}")
            return False
    
    def run(self):
        """Main processing loop"""
        logger.info("Starting CDR Aggregator...")
        logger.info(f"Voice rate: {self.voice_rate_zar_per_min} ZAR/min")
        logger.info(f"Data rate: {self.data_rate_zar_per_gb} ZAR/GB")
        
        try:
            while True:
                # Poll for messages
                msg = self.consumer.poll(1.0)
                
                if msg:
                    self.process_message(msg)
                
                # Log progress every 50 messages
                if self.message_count > 0 and self.message_count % 50 == 0:
                    logger.info(
                        f"Progress: {self.message_count} messages | "
                        f"Data: {self.data_records_processed} | "
                        f"Voice: {self.voice_records_processed} | "
                        f"Writes: {self.scylla_writes} | "
                        f"Failures: {self.write_failures} | "
                        f"Duplicates: {self.duplicates_skipped}"
                    )
                
        except KeyboardInterrupt:
            logger.info("Shutting down...")
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
        finally:
            self.shutdown()
    
    def shutdown(self):
        """Clean shutdown"""
        logger.info("Shutting down CDR Aggregator...")
        
        if hasattr(self, 'consumer'):
            try:
                self.consumer.close()
                logger.info("Kafka consumer closed")
            except Exception as e:
                logger.error(f"Error closing Kafka consumer: {e}")
        
        if hasattr(self, 'cluster'):
            try:
                self.cluster.shutdown()
                logger.info("ScyllaDB connection closed")
            except Exception as e:
                logger.error(f"Error closing ScyllaDB connection: {e}")
        
        logger.info("=== Final Statistics ===")
        logger.info(f"Total messages processed: {self.message_count}")
        logger.info(f"CDR Data records: {self.data_records_processed}")
        logger.info(f"CDR Voice records: {self.voice_records_processed}")
        logger.info(f"ScyllaDB writes: {self.scylla_writes}")
        logger.info(f"Write failures: {self.write_failures}")
        logger.info(f"Duplicates skipped: {self.duplicates_skipped}")

def main():
    """Main entry point"""
    aggregator = None
    try:
        aggregator = CDRAggregator()
        aggregator.run()
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        sys.exit(1)
    finally:
        if aggregator:
            aggregator.shutdown()

if __name__ == "__main__":
    main()