#!/usr/bin/env python3
"""Usage API - TBD 5 - REST API for querying CDR usage data"""

import os
import logging
from datetime import datetime
from flask import Flask, request, jsonify
from flask_httpauth import HTTPBasicAuth
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import sys

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

# Initialize Flask app
app = Flask(__name__)
auth = HTTPBasicAuth()

# Configuration from environment variables
SCYLLA_HOST = os.getenv('SCYLLA_HOST', 'scylladb')
SCYLLA_PORT = int(os.getenv('SCYLLA_PORT', '9042'))
SCYLLA_USER = os.getenv('SCYLLA_USER', 'cassandra')
SCYLLA_PASS = os.getenv('SCYLLA_PASS', 'cassandra')
API_USER = os.getenv('API_USER', 'admin')
API_PASS = os.getenv('API_PASS', 'password')
API_PORT = int(os.getenv('API_PORT', '18089'))

logger.info(f"ScyllaDB: {SCYLLA_HOST}:{SCYLLA_PORT}")
logger.info(f"API User: {API_USER}")

class ScyllaDBConnector:
    """Handle ScyllaDB connections and queries"""
    
    def __init__(self):
        try:
            auth_provider = PlainTextAuthProvider(
                username=SCYLLA_USER,
                password=SCYLLA_PASS
            )
            
            self.cluster = Cluster(
                [SCYLLA_HOST],
                auth_provider=auth_provider,
                port=SCYLLA_PORT,
                connect_timeout=10
            )
            
            self.session = self.cluster.connect()
            self.session.set_keyspace('cdr_analytics')
            logger.info("Connected to ScyllaDB")
            
        except Exception as e:
            logger.error(f"Failed to connect to ScyllaDB: {e}")
            raise
    
    def get_usage_by_msisdn(self, msisdn, start_date, end_date):
        """
        Query usage data for a given MSISDN and date range
        from confluent_kafka import Consumer, KafkaError

consumer = Consumer({
    'bootstrap.servers': 'redpanda:9092',  # Redpanda broker address
    'group.id': 'cdr-aggregator',           # Consumer group name
    'auto.offset.reset': 'earliest'         # Start from beginning or latest
})
consumer.subscribe(['cdr_data', 'cdr_voice'])  # Topics to listen to



        Returns list of usage records
        """
        try:
            query = """
            SELECT msisdn, date, usage_type, total_up_bytes, total_down_bytes, 
                   total_call_seconds, data_cost, voice_cost, last_updated
            FROM cdr_daily_summary
            WHERE msisdn = %s AND date >= %s AND date <= %s
            """
            
            rows = self.session.execute(query, (msisdn, start_date, end_date))
            return list(rows)
            
        except Exception as e:
            logger.error(f"Query error: {e}")
            return []
    
    def close(self):
        """Close connection"""
        try:
            self.cluster.shutdown()
            logger.info("ScyllaDB connection closed")
        except Exception as e:
            logger.error(f"Error closing connection: {e}")

# Initialize database connector
db = None

@app.before_request
def initialize_db():
    """Initialize database connection on first request"""
    global db
    if db is None:
        db = ScyllaDBConnector()

@auth.verify_password
def verify_password(username, password):
    """Verify basic authentication credentials"""
    if username == API_USER and password == API_PASS:
        return username
    logger.warning(f"Failed authentication attempt: {username}")
    return None

def parse_datetime(datetime_str):
    """Parse datetime string format: YYYYMMDDhhmmss"""
    try:
        return datetime.strptime(datetime_str, '%Y%m%d%H%M%S')
    except Exception as e:
        logger.error(f"Error parsing datetime {datetime_str}: {e}")
        return None

def determine_category(usage_type):
    """Determine if usage type is 'data' or 'call'"""
    voice_types = ['voice', 'video_call']
    if usage_type.lower() in voice_types:
        return 'call'
    return 'data'

def format_usage_response(usage_records, start_datetime, end_datetime):
    """
    Format usage records into the expected API response format
    
    Groups by date and usage_type, determines category and measure
    """
    usage_list = []
    
    for record in usage_records:
        msisdn = record.msisdn
        date_str = record.date
        usage_type = record.usage_type
        
        # Determine category and measure based on usage_type
        category = determine_category(usage_type)
        
        if category == 'data':
            # Data usage: bytes
            total_up = record.total_up_bytes or 0
            total_down = record.total_down_bytes or 0
            total = total_up + total_down
            measure = 'bytes'
        else:
            # Call usage: seconds
            total = record.total_call_seconds or 0
            measure = 'seconds'
        
        # Create usage entry
        usage_entry = {
            'category': category,
            'usage_type': usage_type,
            'total': total,
            'measure': measure,
            'start_time': f"{date_str} 00:00:00"  # Daily summary, so start of day
        }
        
        usage_list.append(usage_entry)
    
    return usage_list

@app.route('/data_usage', methods=['GET'])
@auth.login_required
def get_data_usage():
    """
    REST endpoint to retrieve usage data
    
    Query params:
    - msisdn: Phone number (required)
    - start_time: Start datetime YYYYMMDDhhmmss (required)
    - end_time: End datetime YYYYMMDDhhmmss (required)
    """
    try:
        # Get query parameters
        msisdn = request.args.get('msisdn')
        start_time_str = request.args.get('start_time')
        end_time_str = request.args.get('end_time')
        
        # Validate required parameters
        if not msisdn or not start_time_str or not end_time_str:
            logger.warning("Missing required parameters")
            return jsonify({
                'error': 'Missing required parameters: msisdn, start_time, end_time'
            }), 400
        
        # Parse datetime strings
        start_datetime = parse_datetime(start_time_str)
        end_datetime = parse_datetime(end_time_str)
        
        if not start_datetime or not end_datetime:
            logger.warning(f"Invalid datetime format: {start_time_str}, {end_time_str}")
            return jsonify({
                'error': 'Invalid datetime format. Use YYYYMMDDhhmmss'
            }), 400
        
        # Validate date range
        if start_datetime > end_datetime:
            logger.warning("Start time is after end time")
            return jsonify({
                'error': 'start_time must be before end_time'
            }), 400
        
        logger.info(
            f"Query: MSISDN={msisdn}, Start={start_datetime}, End={end_datetime}"
        )
        
        # Convert to date strings for database query (YYYY-MM-DD format)
        start_date = start_datetime.strftime('%Y-%m-%d')
        end_date = end_datetime.strftime('%Y-%m-%d')
        
        # Query database
        usage_records = db.get_usage_by_msisdn(msisdn, start_date, end_date)
        
        if not usage_records:
            logger.info(f"No usage data found for MSISDN {msisdn}")
            return jsonify({
                'msisdn': msisdn,
                'start_time': start_time_str,
                'end_time': end_time_str,
                'usage': []
            }), 200
        
        # Format response
        usage_list = format_usage_response(usage_records, start_datetime, end_datetime)
        
        response = {
            'msisdn': msisdn,
            'start_time': start_time_str,
            'end_time': end_time_str,
            'usage': usage_list,
            'count': len(usage_list)
        }
        
        logger.info(f"Returned {len(usage_list)} usage records for MSISDN {msisdn}")
        
        return jsonify(response), 200
        
    except Exception as e:
        logger.error(f"Error processing request: {e}")
        return jsonify({
            'error': 'Internal server error'
        }), 500

@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint (no auth required)"""
    try:
        if db:
            # Quick test query
            db.session.execute("SELECT now() FROM system.local")
            return jsonify({'status': 'healthy'}), 200
        else:
            return jsonify({'status': 'initializing'}), 503
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        return jsonify({'status': 'unhealthy'}), 503

@app.errorhandler(404)
def not_found(error):
    """Handle 404 errors"""
    return jsonify({'error': 'Endpoint not found'}), 404

@app.errorhandler(405)
def method_not_allowed(error):
    """Handle 405 errors"""
    return jsonify({'error': 'Method not allowed'}), 405

def shutdown_db():
    """Cleanup on shutdown"""
    global db
    if db:
        db.close()

if __name__ == '__main__':
    try:
        logger.info("Starting Usage API...")
        app.run(
            host='0.0.0.0',
            port=API_PORT,
            debug=False,
            threaded=True
        )
    except KeyboardInterrupt:
        logger.info("Shutting down...")
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        sys.exit(1)
    finally:
        shutdown_db()