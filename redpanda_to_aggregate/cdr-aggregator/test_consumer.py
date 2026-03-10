#!/usr/bin/env python3
"""Simple test to verify Kafka connection and message consumption"""

from confluent_kafka import Consumer, KafkaError
import json

def test_kafka_connection():
    """Test connection to Kafka/Redpanda"""
    consumer = Consumer({
        'bootstrap.servers': 'redpanda-0:9092',
        'group.id': 'test-consumer',
        'auto.offset.reset': 'earliest'
    })
    
    consumer.subscribe(['cdr_data', 'cdr_voice'])
    
    print("Listening for messages... (Ctrl+C to stop)")
    
    try:
        while True:
            msg = consumer.poll(1.0)
            
            if msg is None:
                continue
            
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    print("Reached end of partition")
                else:
                    print(f"Error: {msg.error()}")
            else:
                try:
                    data = json.loads(msg.value().decode('utf-8'))
                    print(f"Topic: {msg.topic()}, Partition: {msg.partition()}")
                    print(f"Message: {json.dumps(data, indent=2)}")
                    print("-" * 50)
                except Exception as e:
                    print(f"Error parsing message: {e}")
                    
    except KeyboardInterrupt:
        print("\nStopped by user")
    finally:
        consumer.close()

if __name__ == "__main__":
    test_kafka_connection()