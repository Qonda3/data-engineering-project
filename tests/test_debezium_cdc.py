"""
Debezium Docker Network Integration Tests

Tests the Docker container network:
- PostgreSQL container (postgres:5432)
- Redpanda cluster (redpanda-0, redpanda-1, redpanda-2)
- Kafka Connect container with Debezium (postgres-kafka-cdc:8083)

All containers communicate on the 'wtc_cap_one' Docker network.
Tests verify this network topology works correctly for CDC.
"""

# Auto-install dependencies if missing
import sys
import subprocess


def check_and_install_dependencies():
    """Check for required packages and install if missing"""
    required_packages = {
        'sqlalchemy': 'sqlalchemy',
        'psycopg2': 'psycopg2-binary',
        'confluent_kafka': 'confluent-kafka',  # Note: underscore in import, hyphen in pip
        'requests': 'requests'
    }

    missing_packages = []

    for import_name, pip_name in required_packages.items():
        try:
            __import__(import_name)
        except ImportError:
            missing_packages.append(pip_name)

    if missing_packages:
        print(f"Installing missing packages: {', '.join(missing_packages)}")
        subprocess.check_call(
            [sys.executable, '-m', 'pip', 'install', '--quiet'] + missing_packages + ['--break-system-packages']
        )
        print("✓ Dependencies installed successfully")

# Run the check
check_and_install_dependencies()

# Now import the packages
import unittest
import logging
import json
import time
import requests
from datetime import datetime
from sqlalchemy import create_engine, text
from confluent_kafka import Consumer, Producer
from confluent_kafka.admin import AdminClient
from typing import Dict, List, Any

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


class DebeziumDockerNetworkTest(unittest.TestCase):
    """
    Tests Debezium CDC in Docker network configuration.

    Architecture:
    - PostgreSQL: postgres:5432 (internal), localhost:15432 (external)
    - Redpanda: redpanda-0:9092 (internal), localhost:19092 (external)
    - Kafka Connect: postgres-kafka-cdc:8083 (internal), localhost:8083 (external)

    Tests use INTERNAL names for container-to-container communication.
    Tests connect from host using EXTERNAL ports.
    """

    @classmethod
    def setUpClass(cls):
        """Verify Docker environment is running"""
        super().setUpClass()

        logger.info("=" * 70)
        logger.info("Verifying Docker Container Network")
        logger.info("=" * 70)

        # External connection configs (from test host to containers)
        cls.postgres_external = {
            "host": "localhost",
            "port": 15432,
            "database": "wtc_prod",
            "user": "postgres",
            "password": "postgres",
        }

        cls.redpanda_external = ["localhost:19092", "localhost:29092", "localhost:39092"]
        cls.kafka_connect_external = "http://localhost:8083"

        # Internal connection configs (container-to-container on Docker network)
        # These are what Debezium uses inside the Docker network
        cls.postgres_internal = {
            "hostname": "postgres",  # Docker service name
            "port": "5432",          # Internal port
            "database": "wtc_prod",
            "user": "postgres",
            "password": "postgres",
        }

        cls.redpanda_internal = "redpanda-0:9092"  # Internal broker address

        # Test name prefix (for isolation)
        cls.test_prefix = "test_cdc"
        cls.connector_name = f"{cls.test_prefix}_connector"

        # Verify containers are running
        cls._verify_docker_containers()

        # Setup connections from test host
        cls._setup_external_connections()

    @classmethod
    def _verify_docker_containers(cls):
        """Verify required Docker containers are running"""
        required_containers = ["postgres", "redpanda-0", "postgres-kafka-cdc"]

        try:
            result = subprocess.run(
                ["docker", "ps", "--format", "{{.Names}}"],
                capture_output=True,
                text=True,
                check=True
            )
            running_containers = result.stdout.strip().split('\n')

            for container in required_containers:
                if container not in running_containers:
                    raise unittest.SkipTest(
                        f"Container '{container}' is not running. "
                        f"Start with: docker-compose up -d"
                    )

            logger.info(f"✓ All required containers running: {required_containers}")

        except subprocess.CalledProcessError:
            raise unittest.SkipTest("Docker is not available or accessible")

    @classmethod
    def _setup_external_connections(cls):
        """Setup connections from test host to Docker containers"""
        # PostgreSQL connection (external)
        connection_string = (
            f"postgresql://{cls.postgres_external['user']}:{cls.postgres_external['password']}"
            f"@{cls.postgres_external['host']}:{cls.postgres_external['port']}"
            f"/{cls.postgres_external['database']}"
        )
        cls.engine = create_engine(connection_string)

        # Test PostgreSQL
        try:
            with cls.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            logger.info("✓ PostgreSQL accessible from test host")
        except Exception as e:
            raise unittest.SkipTest(f"Cannot connect to PostgreSQL: {e}")

        # Test Redpanda (external) using confluent_kafka
        try:
            cls.admin_client = AdminClient({
                'bootstrap.servers': ','.join(cls.redpanda_external)
            })
            # Test connection by getting cluster metadata
            metadata = cls.admin_client.list_topics(timeout=10)
            logger.info("✓ Redpanda accessible from test host")
        except Exception as e:
            raise unittest.SkipTest(f"Cannot connect to Redpanda: {e}")
        
        # Test Kafka Connect (external)
        try:
            response = requests.get(cls.kafka_connect_external, timeout=5)
            if response.status_code == 200:
                logger.info("✓ Kafka Connect accessible from test host")
            else:
                raise unittest.SkipTest("Kafka Connect not responding")
        except Exception as e:
            raise unittest.SkipTest(f"Cannot connect to Kafka Connect: {e}")
    
    @classmethod
    def tearDownClass(cls):
        """Cleanup connections"""
        if hasattr(cls, 'engine'):
            cls.engine.dispose()
        # Note: confluent_kafka AdminClient doesn't have close() method
    
    def setUp(self):
        """Setup for each test"""
        # Create Confluent Kafka consumer with proper config dictionary
        self.consumer = Consumer({
            'bootstrap.servers': ','.join(self.redpanda_external),
            'group.id': f'test_group_{int(time.time())}',
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False,
        })
        
        # Clean up any existing test connector
        self._delete_connector()
    
    def tearDown(self):
        """Cleanup after each test"""
        if hasattr(self, 'consumer'):
            self.consumer.close()
        
        # Always clean up test connector and data
        self._delete_connector()
        self._cleanup_test_data()
    
    def _get_connector_config(self) -> Dict:
        """
        Get Debezium connector config using INTERNAL Docker network addresses.
        
        Critical: Debezium runs INSIDE postgres-kafka-cdc container, so it must use
        internal Docker network names (postgres:5432, redpanda-0:9092) not localhost.
        """
        return {
            "name": self.connector_name,
            "config": {
                "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
                
                # INTERNAL addresses (container-to-container on wtc_cap_one network)
                "database.hostname": self.postgres_internal["hostname"],
                "database.port": self.postgres_internal["port"],
                "database.user": self.postgres_internal["user"],
                "database.password": self.postgres_internal["password"],
                "database.dbname": self.postgres_internal["database"],
                
                # Logical decoding config
                "plugin.name": "pgoutput",
                "slot.name": f"{self.test_prefix}_slot",
                "publication.name": f"{self.test_prefix}_publication",
                "publication.autocreate.mode": "filtered",
                
                # Topic naming
                "topic.prefix": self.test_prefix,
                "database.server.name": "postgres",
                
                # JSON serialization (no schema registry needed)
                "key.converter": "org.apache.kafka.connect.json.JsonConverter",
                "value.converter": "org.apache.kafka.connect.json.JsonConverter",
                "key.converter.schemas.enable": "false",
                "value.converter.schemas.enable": "false",
                
                # Only capture new changes (no snapshot of existing data)
                "snapshot.mode": "never",
                
                # Tables to capture
                "table.include.list": "crm_system.accounts,crm_system.addresses,crm_system.devices",
                
                # Single task for testing
                "tasks.max": "1",
            }
        }
    
    def _deploy_connector(self) -> Dict:
        """Deploy connector using Kafka Connect REST API"""
        config = self._get_connector_config()
        
        response = requests.post(
            f"{self.kafka_connect_external}/connectors",
            json=config,
            headers={"Content-Type": "application/json"},
            timeout=30
        )
        
        if response.status_code == 409:
            # Already exists, get status
            return self._get_connector_status()
        
        response.raise_for_status()
        logger.info(f"✓ Deployed connector: {self.connector_name}")
        return response.json()
    
    def _delete_connector(self):
        """Delete connector and cleanup PostgreSQL resources"""
        try:
            # Delete connector via REST API
            response = requests.delete(
                f"{self.kafka_connect_external}/connectors/{self.connector_name}",
                timeout=10
            )
            
            if response.status_code in [200, 204]:
                logger.info(f"✓ Deleted connector: {self.connector_name}")
                time.sleep(3)  # Wait for cleanup
            
            # Clean up PostgreSQL replication slot
            with self.engine.connect() as conn:
                # Check if slot exists
                result = conn.execute(text("""
                    SELECT slot_name FROM pg_replication_slots 
                    WHERE slot_name = :slot_name
                """), {"slot_name": f"{self.test_prefix}_slot"})
                
                if result.fetchone():
                    # Drop slot
                    conn.execute(text(f"""
                        SELECT pg_drop_replication_slot('{self.test_prefix}_slot')
                    """))
                    conn.commit()
                    logger.info("✓ Cleaned up replication slot")
            
            # Clean up publication
            with self.engine.connect() as conn:
                conn.execute(text(f"""
                    DROP PUBLICATION IF EXISTS {self.test_prefix}_publication
                """))
                conn.commit()
                logger.info("✓ Cleaned up publication")
                
        except Exception as e:
            logger.debug(f"Cleanup: {e}")
    
    def _get_connector_status(self) -> Dict:
        """Get connector status"""
        response = requests.get(
            f"{self.kafka_connect_external}/connectors/{self.connector_name}/status",
            timeout=10
        )
        response.raise_for_status()
        return response.json()
    
    def _wait_for_connector_running(self, max_wait: int = 60) -> bool:
        """Wait for connector to reach RUNNING state"""
        start_time = time.time()
        
        while time.time() - start_time < max_wait:
            try:
                status = self._get_connector_status()
                state = status.get("connector", {}).get("state", "UNKNOWN")
                
                if state == "RUNNING":
                    tasks = status.get("tasks", [])
                    if all(t.get("state") == "RUNNING" for t in tasks):
                        logger.info("✓ Connector is RUNNING")
                        return True
                
                if state == "FAILED":
                    logger.error(f"Connector FAILED: {status}")
                    return False
                    
            except Exception:
                pass
            
            time.sleep(2)
        
        return False
    
    def _cleanup_test_data(self):
        """Delete test data from PostgreSQL"""
        try:
            with self.engine.connect() as conn:
                # Delete test accounts (IDs starting with 99xxx)
                conn.execute(text("""
                    DELETE FROM crm_system.devices WHERE account_id >= 99000
                """))
                conn.execute(text("""
                    DELETE FROM crm_system.addresses WHERE account_id >= 99000
                """))
                conn.execute(text("""
                    DELETE FROM crm_system.accounts WHERE account_id >= 99000
                """))
                conn.commit()
                logger.info("✓ Cleaned up test data")
        except Exception as e:
            logger.debug(f"Cleanup test data: {e}")
    
    def _wait_for_messages(self, topic: str, expected_count: int = 1, timeout: int = 30) -> List:
        """Wait for messages from Kafka topic using confluent_kafka"""
        self.consumer.subscribe([topic])
        messages = []
        start_time = time.time()
        
        logger.info(f"Waiting for {expected_count} messages from '{topic}'...")
        
        while len(messages) < expected_count:
            if time.time() - start_time > timeout:
                raise TimeoutError(
                    f"Timeout waiting for messages. Expected {expected_count}, got {len(messages)}"
                )
            
            # Poll for messages (confluent_kafka returns single message or None)
            msg = self.consumer.poll(timeout=1.0)
            
            if msg is None:
                continue
            if msg.error():
                logger.warning(f"Consumer error: {msg.error()}")
                continue
                
            # Decode message value and create wrapper object
            try:
                value = msg.value().decode('utf-8') if msg.value() else None
                if value:
                    # Create simple message wrapper for compatibility
                    class MessageWrapper:
                        def __init__(self, val):
                            self.value = val
                    
                    messages.append(MessageWrapper(value))
                    logger.info(f"Received message {len(messages)}/{expected_count}")
            except Exception as e:
                logger.warning(f"Error decoding message: {e}")
        
        return messages[:expected_count]
    
    # ========================================================================
    # TEST 1: Docker Network Connectivity
    # ========================================================================
    
    def test_postgres_accessible_from_kafka_connect_container(self):
        """
        Test that Kafka Connect container can reach PostgreSQL container.
        
        This is critical - Debezium runs INSIDE postgres-kafka-cdc and must be able
        to connect to postgres:5432 on the Docker network.
        """
        config = self._get_connector_config()["config"]
        
        # Validate config (this attempts connection from postgres-kafka-cdc to postgres)
        response = requests.put(
            f"{self.kafka_connect_external}/connector-plugins/"
            f"io.debezium.connector.postgresql.PostgresConnector/config/validate",
            json=config,
            headers={"Content-Type": "application/json"},
            timeout=15
        )
        
        self.assertEqual(response.status_code, 200, 
                        "Kafka Connect should be able to validate PostgreSQL connection")
        
        validation = response.json()
        error_count = validation.get("error_count", 0)
        
        # If there are errors, log them
        if error_count > 0:
            for item in validation.get("configs", []):
                if item.get("value", {}).get("errors"):
                    logger.error(f"Validation error: {item['value']['name']}: {item['value']['errors']}")
        
        self.assertEqual(error_count, 0,
                        f"PostgreSQL should be accessible from Kafka Connect container. "
                        f"Check Docker network 'wtc_cap_one' connectivity.")
        
        logger.info("✓ Kafka Connect can connect to PostgreSQL on Docker network")
    
    def test_redpanda_accessible_from_kafka_connect_container(self):
        """
        Test that Kafka Connect container can reach Redpanda cluster.
        
        Kafka Connect must be able to write to redpanda-0:9092 on Docker network.
        """
        # Check Kafka Connect can see Redpanda brokers
        response = requests.get(
            f"{self.kafka_connect_external}/",
            timeout=5
        )
        
        self.assertEqual(response.status_code, 200)
        
        info = response.json()
        kafka_cluster_id = info.get("kafka_cluster_id")
        
        # Kafka Connect can only report cluster ID if it can connect to Kafka/Redpanda
        self.assertIsNotNone(kafka_cluster_id, 
                            "Kafka Connect should be connected to Redpanda cluster")
        
        logger.info(f"✓ Kafka Connect connected to Redpanda cluster: {kafka_cluster_id}")
    
    # ========================================================================
    # TEST 2: Connector Deployment in Docker Environment
    # ========================================================================
    
    def test_connector_deploys_successfully_in_docker_network(self):
        """Test deploying Debezium connector in Docker network"""
        result = self._deploy_connector()
        
        self.assertIsNotNone(result)
        self.assertEqual(result["name"], self.connector_name)
        
        # Verify connector reaches RUNNING state
        is_running = self._wait_for_connector_running(max_wait=60)
        self.assertTrue(is_running, 
                       "Connector should reach RUNNING state in Docker network")
        
        # Verify replication slot was created in PostgreSQL
        with self.engine.connect() as conn:
            result = conn.execute(text("""
                SELECT slot_name, active FROM pg_replication_slots 
                WHERE slot_name = :slot_name
            """), {"slot_name": f"{self.test_prefix}_slot"})
            
            slot = result.fetchone()
            self.assertIsNotNone(slot, "Replication slot should be created")
            self.assertTrue(slot[1], "Replication slot should be active")
        
        logger.info("✓ Connector deployed and connected to PostgreSQL via Docker network")
    
    # ========================================================================
    # TEST 3: Data Streaming Through Docker Network
    # ========================================================================
    
    def test_insert_streams_from_postgres_to_redpanda(self):
        """
        Test complete data flow: PostgreSQL → Debezium → Redpanda.
        
        This is the end-to-end test of your Docker network architecture.
        """
        # Deploy connector
        self._deploy_connector()
        self.assertTrue(self._wait_for_connector_running(max_wait=60))
        time.sleep(5)  # Extra time for initialization
        
        # Insert test data into PostgreSQL
        test_account_id = 99001
        topic = f"{self.test_prefix}.crm_system.accounts"
        
        with self.engine.connect() as conn:
            conn.execute(text("""
                INSERT INTO crm_system.accounts 
                (account_id, owner_name, email, phone_number, modified_ts)
                VALUES (:id, :name, :email, :phone, :ts)
            """), {
                "id": test_account_id,
                "name": "Docker Network Test",
                "email": "docker@test.com",
                "phone": "+27999999999",
                "ts": datetime.now()
            })
            conn.commit()
        
        logger.info(f"Inserted test account {test_account_id} into PostgreSQL container")
        
        # Wait for CDC event in Redpanda
        messages = self._wait_for_messages(topic, expected_count=1, timeout=30)
        
        self.assertEqual(len(messages), 1, "Should receive CDC event in Redpanda")
        
        # Parse and validate event
        event = json.loads(messages[0].value)
        payload = event["payload"]
        
        self.assertEqual(payload["op"], "c", "Should be INSERT operation")
        
        after = payload["after"]
        self.assertEqual(after["account_id"], test_account_id)
        self.assertEqual(after["owner_name"], "Docker Network Test")
        self.assertEqual(after["email"], "docker@test.com")
        
        logger.info("✓ Data successfully streamed: PostgreSQL → Debezium → Redpanda")
    
    def test_update_streams_through_docker_network(self):
        """Test UPDATE operation flows through Docker network"""
        self._deploy_connector()
        self.assertTrue(self._wait_for_connector_running(max_wait=60))
        time.sleep(5)
        
        test_account_id = 99002
        topic = f"{self.test_prefix}.crm_system.accounts"
        
        # Insert
        with self.engine.connect() as conn:
            conn.execute(text("""
                INSERT INTO crm_system.accounts 
                (account_id, owner_name, email, phone_number, modified_ts)
                VALUES (:id, :name, :email, :phone, :ts)
            """), {
                "id": test_account_id,
                "name": "Original",
                "email": "original@test.com",
                "phone": "+27888888888",
                "ts": datetime.now()
            })
            conn.commit()
        
        time.sleep(2)
        
        # Update
        with self.engine.connect() as conn:
            conn.execute(text("""
                UPDATE crm_system.accounts 
                SET owner_name = :name, email = :email
                WHERE account_id = :id
            """), {
                "id": test_account_id,
                "name": "Updated",
                "email": "updated@test.com"
            })
            conn.commit()
        
        # Wait for events
        messages = self._wait_for_messages(topic, expected_count=2, timeout=30)
        
        # Find UPDATE event
        update_event = None
        for msg in messages:
            event = json.loads(msg.value)
            if event["payload"]["op"] == "u":
                update_event = event
                break
        
        self.assertIsNotNone(update_event, "Should receive UPDATE event")
        
        payload = update_event["payload"]
        self.assertEqual(payload["before"]["owner_name"], "Original")
        self.assertEqual(payload["after"]["owner_name"], "Updated")
        
        logger.info("✓ UPDATE event streamed correctly through Docker network")
    
    def test_delete_streams_through_docker_network(self):
        """Test DELETE operation flows through Docker network"""
        self._deploy_connector()
        self.assertTrue(self._wait_for_connector_running(max_wait=60))
        time.sleep(5)
        
        test_account_id = 99003
        topic = f"{self.test_prefix}.crm_system.accounts"
        
        # Insert
        with self.engine.connect() as conn:
            conn.execute(text("""
                INSERT INTO crm_system.accounts 
                (account_id, owner_name, email, phone_number, modified_ts)
                VALUES (:id, :name, :email, :phone, :ts)
            """), {
                "id": test_account_id,
                "name": "To Delete",
                "email": "delete@test.com",
                "phone": "+27777777777",
                "ts": datetime.now()
            })
            conn.commit()
        
        time.sleep(2)
        
        # Delete
        with self.engine.connect() as conn:
            conn.execute(text("""
                DELETE FROM crm_system.accounts WHERE account_id = :id
            """), {"id": test_account_id})
            conn.commit()
        
        # Wait for events
        messages = self._wait_for_messages(topic, expected_count=2, timeout=30)
        
        # Find DELETE event
        delete_event = None
        for msg in messages:
            event = json.loads(msg.value)
            if event["payload"]["op"] == "d":
                delete_event = event
                break
        
        self.assertIsNotNone(delete_event, "Should receive DELETE event")
        
        payload = delete_event["payload"]
        self.assertEqual(payload["before"]["account_id"], test_account_id)
        self.assertIsNone(payload["after"], "DELETE after should be null")
        
        logger.info("✓ DELETE event streamed correctly through Docker network")
    
    def test_all_three_tables_stream_independently(self):
        """Test that all CRM tables stream to separate topics"""
        self._deploy_connector()
        self.assertTrue(self._wait_for_connector_running(max_wait=60))
        time.sleep(5)
        
        test_account_id = 99004
        
        # Insert into all three tables
        with self.engine.connect() as conn:
            # Accounts
            conn.execute(text("""
                INSERT INTO crm_system.accounts 
                (account_id, owner_name, email, phone_number, modified_ts)
                VALUES (:id, :name, :email, :phone, :ts)
            """), {
                "id": test_account_id,
                "name": "Multi Table",
                "email": "multi@test.com",
                "phone": "+27666666666",
                "ts": datetime.now()
            })
            
            # Addresses
            conn.execute(text("""
                INSERT INTO crm_system.addresses 
                (account_id, street_address, city, state, postal_code, country, modified_ts)
                VALUES (:id, :street, :city, :state, :postal, :country, :ts)
            """), {
                "id": test_account_id,
                "street": "123 Docker St",
                "city": "Container City",
                "state": "WC",
                "postal": "8001",
                "country": "ZAR",
                "ts": datetime.now()
            })
            
            # Devices
            conn.execute(text("""
                INSERT INTO crm_system.devices 
                (device_id, account_id, device_name, device_type, device_os, modified_ts)
                VALUES (:device_id, :account_id, :name, :type, :os, :ts)
            """), {
                "device_id": 99004,
                "account_id": test_account_id,
                "name": "Test Device",
                "type": "Mobile",
                "os": "iOS",
                "ts": datetime.now()
            })
            
            conn.commit()
        
        # Verify each topic receives events
        tables = ["accounts", "addresses", "devices"]
        
        for table in tables:
            topic = f"{self.test_prefix}.crm_system.{table}"
            
            # Create new consumer for each topic
            consumer = Consumer({
                'bootstrap.servers': ','.join(self.redpanda_external),
                'group.id': f'test_group_{table}_{int(time.time())}',
                'auto.offset.reset': 'earliest',
                'enable.auto.commit': False,
            })
            
            try:
                consumer.subscribe([topic])
                messages = []
                start_time = time.time()
                
                # Wait for message from this topic
                while len(messages) < 1 and (time.time() - start_time) < 20:
                    msg = consumer.poll(timeout=1.0)
                    
                    if msg is None:
                        continue
                    if msg.error():
                        continue
                    
                    value = msg.value().decode('utf-8') if msg.value() else None
                    if value:
                        messages.append(value)
                
                self.assertGreater(len(messages), 0, f"Should receive event from {table}")
                
                event = json.loads(messages[0])
                source_table = event["payload"]["source"]["table"]
                self.assertEqual(source_table, table)
                
                logger.info(f"✓ Table '{table}' streaming to {topic}")
                
            finally:
                consumer.close()
        
        logger.info("✓ All three CRM tables streaming independently through Docker network")


if __name__ == '__main__':
    unittest.main(verbosity=2)