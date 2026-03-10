from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from faker import Faker
from faker.providers import phone_number
from faker.providers import file
import random
import time
from datetime import datetime
from datetime import timedelta
from pathlib import PosixPath

import logging
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',datefmt='%Y-%m-%d %H:%M:%S ')

import os
environment = 'dev' if os.getenv('USER', '') != '' else 'prod'

# Config data
if environment == 'dev':
    DATABASE_URL = "postgresql://postgres:postgres@localhost:15432/wtc_prod"
else:
    DATABASE_URL = "postgresql://postgres:postgres@postgres:5432/wtc_prod"

if environment == 'dev':
    TOTAL_SECONDS=86400
    TOTAL_TRANSACTIONS=5000
else:
    TOTAL_SECONDS=86400*2
    TOTAL_TRANSACTIONS=5000*10

logger.info(f"TOTAL_SECONDS={TOTAL_SECONDS}")
logger.info(f"TOTAL_TRANSACTIONS={TOTAL_TRANSACTIONS}")

INTERVAL_TIME_SEC=round((TOTAL_SECONDS/(TOTAL_TRANSACTIONS)), 0)/3
logger.info(f"Interval time [{INTERVAL_TIME_SEC}]")
if INTERVAL_TIME_SEC == 0:
    raise Exception(f"INTERVAL_TIME_SEC can not be 0")

MAX_ACCOUNT_ID=99999
MAX_DEVICE_ID=99999

logger.info('Sleeping 5secs to wait for postgresql startup...')
time.sleep(5)

engine = create_engine(DATABASE_URL)
Session = sessionmaker(bind=engine)
session = Session()

fake = Faker()
fake.add_provider(phone_number)
fake.add_provider(file)
Faker.seed(418001)
random.seed(27418001)

MSISDN_COUNT=50000
POST_CODE_COUNT=20000

MSISDNS=[fake.msisdn() for _ in range(MSISDN_COUNT)]
POST_CODES=[fake.postcode().replace("'", "") for _ in range(POST_CODE_COUNT)]

def store_sql(line):
    with open("sql_queries.dat", mode='a') as f:
        f.write(f"{line}\n")
        f.close()


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


def gen_account_sql(fake, account_id, file_datetime):
    logger.debug('Updating account')
    owner_name = fake.name().replace("'", "")
    email = fake.email().replace("'", "")
    phone_number = random.choice(MSISDNS)
    sql = f"""
        INSERT INTO crm_system.accounts (account_id, owner_name, email, phone_number, modified_ts)
        VALUES ({account_id}, '{owner_name}', '{email}', '{phone_number}', '{file_datetime.strftime('%Y-%m-%d %H:%M:%S.%f')}')
        ON CONFLICT (account_id) DO UPDATE
        SET owner_name = EXCLUDED.owner_name, email = EXCLUDED.email, phone_number = EXCLUDED.phone_number, modified_ts = EXCLUDED.modified_ts;
        """
    
    return sql

def gen_address_sql(fake, account_id, file_datetime):
    logger.debug('Updating address')
    street_address = fake.street_address().replace("'", "")
    city = fake.city().replace("'", "")
    state = random.choice(["EC", "FS", "GP", "KZN", "LP", "MP", "NC", "NW", "WC"])
    postal_code = random.choice(POST_CODES)
    country = "ZAR"

    sql = f"""
        INSERT INTO crm_system.addresses (account_id, street_address, city, state, postal_code, country, modified_ts)
        VALUES ({account_id}, '{street_address}', '{city}', '{state}', '{postal_code}', '{country}', '{file_datetime.strftime('%Y-%m-%d %H:%M:%S.%f')}')
        ON CONFLICT (account_id) DO UPDATE
        SET street_address = EXCLUDED.street_address, city = EXCLUDED.city, state = EXCLUDED.state, 
        postal_code = EXCLUDED.postal_code, country = EXCLUDED.country, modified_ts = EXCLUDED.modified_ts;
        """
    
    return sql

def gen_device_sql(MAX_DEVICE_ID, account_id, file_datetime):
    logger.debug('Updating device')
    device_id = random.randint(10000, MAX_DEVICE_ID)  # Random unique device ID
    device_name = random.choice(['iPhone 14 Pro Max', 'iPhone 14 Pro', 'iPhone 14', 'iPhone 13 Pro Max', 'iPhone 13 Pro', 'iPhone 13', 'iPhone 12 Pro Max', 'iPhone 12 Pro', 
            'iPhone 12', 'iPhone 11 Pro Max', 'iPhone 11 Pro', 'iPhone 11', 'Samsung Galaxy S23 Ultra', 'Samsung Galaxy S23+', 'Samsung Galaxy S23', 
            'Samsung Galaxy S22 Ultra', 'Samsung Galaxy S22+', 'Samsung Galaxy S22', 'Samsung Galaxy Note 20 Ultra', 'Samsung Galaxy Note 20', 
            'Samsung Galaxy Z Fold 4', 'Samsung Galaxy Z Flip 4', 'Samsung Galaxy Z Fold 3', 'Samsung Galaxy Z Flip 3', 'Samsung Galaxy A54', 
            'Samsung Galaxy A53', 'Samsung Galaxy A52', 'OnePlus 11', 'OnePlus 10 Pro', 'OnePlus 9 Pro', 'OnePlus 9', 'OnePlus Nord 2', 
            'Google Pixel 7 Pro', 'Google Pixel 7', 'Google Pixel 6 Pro', 'Google Pixel 6', 'Google Pixel 5', 'Xiaomi 13 Pro', 'Xiaomi 13', 
            'Xiaomi 12 Pro', 'Xiaomi 12', 'Xiaomi Mi 11 Ultra', 'Xiaomi Mi 11', 'Oppo Find X5 Pro', 'Oppo Find X5', 'Oppo Reno 8 Pro', 'Oppo Reno 8', 
            'Vivo X80 Pro', 'Vivo X80', 'Vivo X70 Pro+', 'Sony Xperia 1 IV', 'Sony Xperia 5 IV', 'Motorola Edge 30 Pro', 'Motorola Edge 30', 
            'Motorola Razr 2022', 'Huawei P50 Pro', 'Huawei Mate 40 Pro', 'Huawei Mate 40', 'Nokia G60 5G', 'Nokia X20', 'TCL Stylus 5G', 
            'Realme GT 2 Pro', 'Realme GT 2', 'Realme 9 Pro+', 'Asus ROG Phone 6 Pro', 'Asus ROG Phone 6', 'Honor Magic4 Pro', 'Honor V40 Pro', 
            'Honor 50 Pro', 'Infinix Zero 5G', 'Infinix Note 11 Pro', 'Sharp Aquos R6', 'LG V60 ThinQ', 'LG Wing', 'LG Velvet', 'ZTE Axon 20', 
            'ZTE Axon 30 Ultra', 'ZTE Axon 30', 'BlackBerry Key2', 'BlackBerry Key2 LE', 'Alcatel 3X (2019)', 'Alcatel 1S (2021)', 'Gionee M12', 
            'Gionee F9 Plus', 'Micromax IN Note 1', 'Micromax IN 1B', 'Lava Agni 5G', 'Lava Z2', 'Panasonic Eluga I8', 'Panasonic Eluga Ray 800', 
            'Karbonn Titanium Jumbo', 'Karbonn Smart A12 Star', 'Celkon Millennia Q450', 'Celkon Campus A35K', 'Spice F302', 'Spice Stellar Mi-435', 
            'Videocon A55HD', 'Videocon V50FG', 'XOLO Era 4X', 'XOLO Black 1X'])
    device_type = random.choice(["Mobile", "Tablet", "Laptop", "Desktop"])
    device_os = random.choice(["iOS", "Android", "Windows", "macOS", "Linux"])

    sql = f"""
        INSERT INTO crm_system.devices (device_id, account_id, device_name, device_type, device_os, modified_ts)
        VALUES ({device_id}, {account_id}, '{device_name}', '{device_type}', '{device_os}', '{file_datetime.strftime('%Y-%m-%d %H:%M:%S.%f')}')
        ON CONFLICT (device_id) DO UPDATE
        SET device_name = EXCLUDED.device_name, device_type = EXCLUDED.device_type, device_os = EXCLUDED.device_os, modified_ts = EXCLUDED.modified_ts;
        """
    
    return sql

fake = Faker('en-uk')
modified_datetime = datetime(2024, 1, 1, 0, 0, 0)
last_idx = read_last_idx()
logger.debug(f"Loaded last_idx: {last_idx}")
STARTING=True
event_counter = 0
for idx in range(TOTAL_TRANSACTIONS):
    run_active = (not idx < last_idx)
    if run_active and STARTING:
        logger.info(f'Starting at idx: {idx}')
        STARTING=False

    with engine.connect() as connection:
        account_id = random.randint(10000, MAX_ACCOUNT_ID)

        time_increment = (random.randint(1, 500))
        modified_datetime = modified_datetime + timedelta(seconds=INTERVAL_TIME_SEC, milliseconds=time_increment)
        sql = gen_account_sql(fake, account_id, modified_datetime)
        if run_active:
            connection.execute(text(sql))
            connection.commit()
            event_counter += 1

        time_increment = (random.randint(1, 500))
        modified_datetime = modified_datetime + timedelta(seconds=INTERVAL_TIME_SEC, milliseconds=time_increment)
        sql = gen_address_sql(fake, account_id, modified_datetime)
        time_increment = (random.randint(1, 10) / 10)
        if run_active:
            connection.execute(text(sql))
            connection.commit()
            event_counter += 1

        time_increment = (random.randint(1, 500))
        modified_datetime = modified_datetime + timedelta(seconds=INTERVAL_TIME_SEC, milliseconds=time_increment)
        sql = gen_device_sql(MAX_DEVICE_ID, account_id, modified_datetime)
        if run_active:
            connection.execute(text(sql))
            connection.commit()
            event_counter += 1
            time.sleep(0.05)

        if idx > 0 and idx % 1000 == 0:
            logger.info('Generated 1000 CRM events...')

        store_idx(idx=idx)
        

logger.info(f"Completed the generation of [{event_counter}] CRM events")
os.unlink('idx_data.dat')
