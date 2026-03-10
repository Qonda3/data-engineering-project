import random as rnd_mrv
import random as rnd_wak
import json
from datetime import datetime, timedelta
import time
from kafka import KafkaProducer
from kafka import KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import KafkaError
import socket
from pathlib import PosixPath

import logging
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',datefmt='%Y-%m-%d %H:%M:%S ')
logging.getLogger('kafka').setLevel(level=logging.ERROR)

import os
environment = 'dev' if os.getenv('USER', '') != '' else 'prod'

if environment == 'dev':
    KAFKA_SERVERS=['localhost:19092','localhost:29092','localhost:39092','localhost:49092','localhost:59092']
    TOPIC_NAME = 'tick-data-dev'
else:
    KAFKA_SERVERS=['redpanda-0:9092','redpanda-1:9092','redpanda-2:9092']
    TOPIC_NAME = 'tick-data'

if environment == 'dev':
    TOTAL_SECONDS=86400*2
    TICK_COUNT=TOTAL_SECONDS*2
else:
    TOTAL_SECONDS=86400*2
    TICK_COUNT=TOTAL_SECONDS*2

logger.info(f"TOTAL_SECONDS={TOTAL_SECONDS}")
logger.info(f"TICK_COUNT={TICK_COUNT}")

parts = str(round((float(TOTAL_SECONDS)/TICK_COUNT), 3)).split('.')
INTERVAL_TIME_SEC=int(parts[0])
INTERVAL_TIME_MS=int(parts[1]) * 100
logger.info(f"Interval time [{INTERVAL_TIME_SEC}] seconds")
logger.info(f"Interval time [{INTERVAL_TIME_MS}] milliseconds")

MRVZAR = "MRVZAR"
WAKMRV = "WAKMRV"
PRODUCE_TO_KAFKA=True

rnd_mrv.seed(27418001)
rnd_wak.seed(948342783)

def on_send_success(record_metadata):
    logger.debug(record_metadata.topic, record_metadata.partition, record_metadata.offset)


def on_send_error(excp):
    logger.error('Send failed for some reason', exc_info=excp)


def connect_to_kafka():
    producer = KafkaProducer(bootstrap_servers=KAFKA_SERVERS, 
                             key_serializer=lambda v: json.dumps(v).encode('utf-8'),
                             value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    logger.info('Producer constructed...')
    return producer


def produce_to_kafka(producer, topic, key, record):
    for _ in range(100):
        future = producer.send(topic, key=key, value=record)
        try:
            rm = future.get(timeout=10)
            logger.debug(f"Produced record: {rm}")
            return
        except KafkaError as ke:
            logger.exception("Failed to produce the message", ke)
            time.sleep(0.5)


def store_idx(idx):
    with open("idx_data.dat", mode='+w') as f:
        f.write(f"{idx}\n")
        f.close()


def read_last_idx() -> int:
    idx_file = PosixPath('idx_data.dat')
    if not idx_file.exists():
        return 0
    
    with open(idx_file, mode='+r') as f:
        lines = f.readlines()
        return int(lines[0])
    

def generate_tick(pair_name, generator, tick_datetime):
    bid_price = round(generator.uniform(1.0, 2.0), 4)
    spread = round(generator.uniform(0.01, 0.05), 4)
    ask_price = round(bid_price + spread, 4)
        
    tick_data = {
            "timestamp": tick_datetime.strftime("%Y-%m-%d %H:%M:%S.%f"),
            "pair_name": pair_name,
            "bid_price": bid_price,
            "ask_price": ask_price,
            "spread": spread
        }
    
    return tick_data

mrv_counter = 0
wak_counter = 0
tick_datetime = datetime(2024, 1, 1, 0, 0, 0)
try:
    producer = connect_to_kafka()
    last_idx = read_last_idx()
    logger.debug(f"Loaded last_idx: {last_idx}")
    STARTING=True
    for idx in range(TICK_COUNT):
        tick_datetime = tick_datetime + timedelta(seconds=INTERVAL_TIME_SEC, milliseconds=INTERVAL_TIME_MS + rnd_mrv.randint(-100, 100))
        run_active = (not idx < last_idx)
        if run_active and STARTING:
            logger.info(f'Starting at idx: {idx}')
            STARTING=False      

        tick_data = generate_tick(MRVZAR, rnd_mrv, tick_datetime)
        if run_active:
            mrv_counter += 1
            if PRODUCE_TO_KAFKA:
                produce_to_kafka(producer=producer, topic=TOPIC_NAME, key=f"{tick_data['pair_name']}-{tick_data['timestamp'][:16]}", record=tick_data)
            store_idx(idx=idx)

        tick_data = generate_tick(WAKMRV, rnd_mrv, tick_datetime)
        if run_active:
            wak_counter += 1
            if PRODUCE_TO_KAFKA:
                produce_to_kafka(producer=producer, topic=TOPIC_NAME, key=f"{tick_data['pair_name']}-{tick_data['timestamp'][:16]}", record=tick_data)
            store_idx(idx=idx)

        if idx > 0 and idx % 100 == 0:
            if run_active:
                if PRODUCE_TO_KAFKA:
                    producer.flush()
                    time.sleep(0.2)

        if idx > 0 and idx % 1000 == 0:
            logger.info('Generated 1000 tick events...')

except KeyboardInterrupt:
    logger.debug("Producer stopped.")

producer.flush(timeout=10)
producer.close()
logger.info(f"Completed tick generation, MRVZAR [{mrv_counter}], WAKMRV [{wak_counter}], Total [{mrv_counter + wak_counter}] datetime [{tick_datetime}]")

os.unlink('idx_data.dat')
