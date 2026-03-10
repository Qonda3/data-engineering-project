import os
from pathlib import Path
import logging
import paramiko
import json
import time
import csv
from confluent_kafka import Producer

ENV = 'dev' if os.getenv('USER', '') else 'prod'

SFTP = {
    'dev': {'hostname': 'localhost', 'port': 10022},
    'prod': {'hostname': 'sftp', 'port': 22},
}

KAFKA = {
    'dev': ['localhost:19092', 'localhost:29092', 'localhost:39092'],
    'prod': ['redpanda-0:9092', 'redpanda-1:9092', 'redpanda-2:9092'],
}

SFTP_DIR = "/home/cdr_data"    # <-- FIXED SFTP PATH

DOWNLOAD_DIR = Path('./cdr_downloads')
STATE_FILE = Path('./processed_files.json')
DOWNLOAD_DIR.mkdir(exist_ok=True)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def load_state():
    if STATE_FILE.exists():
        with open(STATE_FILE) as f:
            return set(json.load(f))
    return set()


def save_state(processed_files):
    with open(STATE_FILE, 'w') as f:
        json.dump(list(processed_files), f)


def connect_sftp():
    config = SFTP[ENV]
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    for attempt in range(5):
        try:
            ssh.connect(
                config['hostname'],
                port=config['port'],
                username='cdr_data',
                password='password',
                timeout=10,
            )
            return ssh.open_sftp(), ssh
        except Exception as e:
            logger.warning(f"SFTP connect attempt {attempt+1}/5 failed: {e}")
            time.sleep(5)

    raise Exception("Failed to connect to SFTP")


def connect_kafka():
    try:
        producer = Producer({"bootstrap.servers": ",".join(KAFKA[ENV])})
        logger.info("Kafka Producer connected")
        return producer
    except Exception as e:
        logger.error(f"Failed to connect Kafka: {e}")
        raise


def list_new_files(sftp, processed_files):
    try:
        files = sftp.listdir(SFTP_DIR)
        new_files = [f for f in files if f.endswith(".csv") and f not in processed_files]
        logger.info(f"Found {len(new_files)} new CSV files")
        return new_files
    except Exception as e:
        logger.error(f"Failed to list SFTP directory {SFTP_DIR}: {e}")
        return []


def download_file(sftp, filename):
    local_path = DOWNLOAD_DIR / filename
    try:
        sftp.get(f"{SFTP_DIR}/{filename}", str(local_path))
        logger.info(f"Downloaded {filename}")
        return local_path
    except Exception as e:
        logger.error(f"Failed to download {filename}: {e}")
        return None


def process_file(sftp, producer, filename, processed_files):
    local_path = download_file(sftp, filename)
    if not local_path:
        return False

    topic = "cdr-voice" if "voice" in filename.lower() else "cdr-data"
    logger.info(f"Using topic: {topic}")

    try:
        with open(local_path, newline='') as csvfile:
            reader = csv.DictReader(csvfile)

            for row in reader:
                producer.produce(topic, json.dumps(row).encode("utf-8"))

        producer.flush()
        processed_files.add(filename)
        save_state(processed_files)

        logger.info(f"Processed {filename} → {topic}")
        return True

    except Exception as e:
        logger.error(f"Error processing {filename}: {e}")
        return False
