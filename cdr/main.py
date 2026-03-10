import csv
from faker import Faker
from faker.providers import phone_number
from faker.providers import file
import random
from datetime import datetime, timedelta
import time
from pathlib import PosixPath
import paramiko
from paramiko import SSHClient
import json
try:
    from confluent_kafka import Producer
    _HAVE_CONFLUENT = True
except Exception:
    Producer = None
    _HAVE_CONFLUENT = False

import logging
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',datefmt='%Y-%m-%d %H:%M:%S ')
logging.getLogger('paramiko').setLevel(level=logging.WARN)

import os
# allow explicit override for easier testing
environment = os.getenv('CDR_ENV', None)
if environment is None:
    environment = 'dev' if os.getenv('USER', '') != '' else 'prod'

# Kafka / Redpanda config
if environment == 'dev':
    KAFKA_BOOTSTRAP = 'localhost:19092,localhost:29092,localhost:39092'
    TOPIC_CDR_DATA = 'cdr-data-dev'
    TOPIC_CDR_VOICE = 'cdr-voice-dev'
else:
    KAFKA_BOOTSTRAP = 'redpanda-0:9092,redpanda-1:9092,redpanda-2:9092'
    TOPIC_CDR_DATA = 'cdr-data'
    TOPIC_CDR_VOICE = 'cdr-voice'

PRODUCE_TO_KAFKA = _HAVE_CONFLUENT
if not PRODUCE_TO_KAFKA:
    logger.warning('confluent_kafka not available; Kafka production disabled')

def delivery_report(err, msg):
    if err is not None:
        logger.error(f"Delivery failed for record {msg.key()}: {err}")
    else:
        logger.debug(f"Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")

def connect_to_kafka():
    try:
        conf = {'bootstrap.servers': KAFKA_BOOTSTRAP}
        p = Producer(conf)
        logger.info('Kafka producer constructed...')
        return p
    except Exception as e:
        logger.error(f'Failed to create Kafka producer: {e}')
        return None

def _serialize_record(record: dict):
    # convert datetime objects to isoformat strings
    out = {}
    for k, v in record.items():
        if isinstance(v, datetime):
            out[k] = v.strftime("%Y-%m-%d %H:%M:%S.%f")
        else:
            out[k] = v
    return out

def produce_records(producer, topic: str, records: list):
    if producer is None:
        logger.debug('No producer available, skipping produce step')
        return
    for rec in records:
        try:
            rec_s = _serialize_record(rec)
            key = rec_s.get('msisdn', None)
            producer.produce(topic=topic, key=str(key).encode('utf-8') if key is not None else None, value=json.dumps(rec_s).encode('utf-8'), callback=delivery_report)
        except Exception as e:
            logger.error(f'Error producing record to kafka: {e}')
    try:
        producer.poll(0)
    except Exception:
        pass

# Config Data
if environment == 'dev':
    SFTP_HOSTNAME = "localhost"  # Replace with your SFTP server address
    SFTP_PORT = 10022  # Default SFTP port
else:
    SFTP_HOSTNAME = "sftp"  # Replace with your SFTP server address
    SFTP_PORT = 22  # Default SFTP port

SFTP_USERNAME = "cdr_data"  # Replace with your SFTP username
SFTP_PASSWORD = "password"  # Replace with your SFTP password
SLEEP_TIME=1
SFTP_ENABLED = os.getenv('SFTP_ENABLED', '1') != '0'
SFTP_FALLBACK_DIR = os.getenv('SFTP_FALLBACK_DIR', './out/sftp')

# Allow quick test overrides for counts
if os.getenv('CDR_TEST') == '1':
    FILE_COUNT = int(os.getenv('CDR_FILE_COUNT', '2'))
    LINE_COUNT = int(os.getenv('CDR_LINE_COUNT', '10'))

if environment == 'dev':
    TOTAL_SECONDS=86400*1
    LINE_COUNT=1000
    LINES_PER_SECOND=1
    FILE_COUNT=int(round((TOTAL_SECONDS*LINES_PER_SECOND)/LINE_COUNT))
else:
    TOTAL_SECONDS=86400*2
    LINE_COUNT=1000
    LINES_PER_SECOND=5
    FILE_COUNT=int(round((TOTAL_SECONDS*LINES_PER_SECOND)/LINE_COUNT))

logger.info(f"TOTAL_SECONDS={TOTAL_SECONDS}")
logger.info(f"LINE_COUNT={LINE_COUNT}")
logger.info(f"LINES_PER_SECOND={LINES_PER_SECOND}")
logger.info(f"FILE_COUNT={FILE_COUNT}")

INTERVAL_TIME_SEC=round((TOTAL_SECONDS/(FILE_COUNT)), 0)
logger.info(f"Interval time [{INTERVAL_TIME_SEC}ms]")
if INTERVAL_TIME_SEC == 0:
    raise Exception(f"INTERVAL_TIME_SEC can not be 0")

fake = Faker()
fake.add_provider(phone_number)
fake.add_provider(file)
Faker.seed(418001)
random.seed(27418001)

MSISDN_COUNT=50000
IP_ADDRESS_COUNT=500000
WEBSITE_URL_COUNT=500000
DEST_NR_COUNT=100000

logger.info("Generating faker data...")
MSISDNS=[fake.msisdn() for _ in range(MSISDN_COUNT)]
IP_ADDRESSES=[fake.ipv4_public() for _ in range(MSISDN_COUNT)]
WEBSITE_URLS=[fake.url() for _ in range(MSISDN_COUNT)]
DEST_NRS=[fake.msisdn() for _ in range(MSISDN_COUNT)]
logger.info("Faker data created...")

cdr_voice_counter = 0
cdr_data_counter = 0

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

def upload_file_to_sftp(local_file, remote_file):
    if not SFTP_ENABLED:
        logger.info('SFTP disabled, performing local fallback copy')
        os.makedirs(SFTP_FALLBACK_DIR, exist_ok=True)
        dest = os.path.join(SFTP_FALLBACK_DIR, os.path.basename(remote_file))
        try:
            from shutil import copyfile
            copyfile(local_file, dest)
            logger.info(f'Copied {local_file} to local fallback {dest}')
            return
        except Exception:
            logger.exception('Failed to copy to local fallback')
            return

    for i in range(10):
        try:
            ssh = SSHClient()
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            ssh.connect(hostname=SFTP_HOSTNAME, port=int(SFTP_PORT), username=SFTP_USERNAME, password=SFTP_PASSWORD, disabled_algorithms={'keys': ['rsa-sha2-256', 'rsa-sha2-512']})
            sftp = ssh.open_sftp()
            sftp.put(local_file, remote_file)
            logger.info(f"File '{local_file}' successfully uploaded to '{remote_file}'.")
            sftp.close()
            ssh.close()
            time.sleep(0.5)
            return
        except Exception as e:
            logger.warning(f"Attempt {i+1}: SFTP upload failed: {e}")
            logger.debug('SFTP exception details:', exc_info=True)
            time.sleep(1)

    # if we get here, all attempts failed — fallback to local copy
    try:
        os.makedirs(SFTP_FALLBACK_DIR, exist_ok=True)
        dest = os.path.join(SFTP_FALLBACK_DIR, os.path.basename(remote_file))
        from shutil import copyfile
        copyfile(local_file, dest)
        logger.info(f'All SFTP attempts failed — copied {local_file} to local fallback {dest}')
    except Exception:
        logger.exception('Failed final fallback copy after SFTP failures')

def generate_cdr_data(file_datetime, num_records):
    data_types = ['video', 'audio', 'image', 'text', 'application']

    cdr_records = []
    for _ in range(num_records):
        record = {
            "msisdn": random.choice(MSISDNS),
            "tower_id": random.randint(1, 2000),
            "up_bytes": random.randint(100000, 1000000),
            "down_bytes": random.randint(100000, 1000000),
            "data_type": random.choice(data_types),
            "ip_address": random.choice(IP_ADDRESSES),
            "website_url": random.choice(WEBSITE_URLS),
            "event_datetime": fake.date_time_between(start_date=file_datetime, end_date=(file_datetime + timedelta(seconds=INTERVAL_TIME_SEC)))
        }
        cdr_records.append(record)

    logger.debug(f"Completed generating [{num_records}] cdr_data records")
    return cdr_records


def generate_cdr_voice(file_datetime, num_records):
    call_types = ['voice', 'video']

    cdr_records = []
    for _ in range(num_records):
        record = {
            "msisdn": random.choice(MSISDNS),
            "tower_id": random.randint(1, 2000),
            "call_type": random.choice(call_types),
            "dest_nr": random.choice(DEST_NRS),
            "call_duration_sec": random.randint(1, 1800),
            "start_time": fake.date_time_between(start_date=file_datetime, end_date=(file_datetime + timedelta(seconds=INTERVAL_TIME_SEC)))
        }
        cdr_records.append(record)

    logger.debug(f"Completed generating [{num_records}] cdr_voice records")
    return cdr_records

file_datetime = datetime(2024, 1, 1, 0, 0, 0)
last_idx = read_last_idx()
logger.info(f"Loaded last_idx: {last_idx}")
STARTING=True
producer = None
if PRODUCE_TO_KAFKA:
    producer = connect_to_kafka()

start_time = None
for idx in range(FILE_COUNT):
    start_time = time.time()
    file_datetime = file_datetime + timedelta(seconds=INTERVAL_TIME_SEC)
    logger.debug(f"Generating data for [{file_datetime}]")
    run_active = (not idx < last_idx)

    if run_active and STARTING:
        logger.info(f'Starting at idx: {idx}')
        STARTING=False

    cdr_data = generate_cdr_data(file_datetime=file_datetime, num_records=LINE_COUNT)
    cdr_data_counter += len(cdr_data)
    cdr_voice = generate_cdr_voice(file_datetime=file_datetime, num_records=LINE_COUNT)
    cdr_voice_counter += len(cdr_voice)

    if run_active:
        with open('cdr_data.csv', mode='w', newline='') as file:
            writer = csv.DictWriter(file, fieldnames=["msisdn", "tower_id", "up_bytes", "down_bytes", "data_type", "ip_address", "website_url", "event_datetime"])
            writer.writeheader()
            writer.writerows(cdr_data)
        
        logger.debug('Completed writing out cdr_data.csv')
        dest_filename = f"cdr_data_{file_datetime.strftime('%Y%m%d_%H%M%S')}.csv"
        upload_file_to_sftp('cdr_data.csv', f"{dest_filename}")
        # produce to kafka
        try:
            produce_records(producer, TOPIC_CDR_DATA, cdr_data)
        except Exception:
            logger.exception('Failed to produce cdr_data records')

        with open('cdr_voice.csv', mode='w', newline='') as file:
            writer = csv.DictWriter(file, fieldnames=["msisdn", "tower_id", "call_type", "dest_nr", "call_duration_sec", "start_time"])
            writer.writeheader()
            writer.writerows(cdr_voice)
        
        logger.debug('Completed writing out cdr_voice.csv')
        dest_filename = f"cdr_voice_{file_datetime.strftime('%Y%m%d_%H%M%S')}.csv"
        upload_file_to_sftp('cdr_voice.csv', f"{dest_filename}")
        try:
            produce_records(producer, TOPIC_CDR_VOICE, cdr_voice)
        except Exception:
            logger.exception('Failed to produce cdr_voice records')

        store_idx(idx=idx)
        elapsed_time = (time.time() - start_time) * 1000
        remaining_time = max(0, (SLEEP_TIME * 1000) - elapsed_time)
        if environment == 'prod':
            time.sleep(0.1)

        logger.info('Generated 2 cdr files...')

logger.info(f'Completed data generation. cdr_data [{cdr_data_counter}], cdr_voice [{cdr_voice_counter}], files [{FILE_COUNT}]')
if producer is not None:
    try:
        producer.flush(timeout=10)
    except Exception:
        logger.exception('Error flushing producer')

if os.path.exists('idx_data.dat'):
    os.unlink('idx_data.dat')
