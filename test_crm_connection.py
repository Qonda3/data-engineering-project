#!/usr/bin/env python3
import psycopg2

try:
    conn = psycopg2.connect(
        host='postgres',
        port='5432',
        database='wtc_analytics',
        user='postgres',
        password='postgres'
    )
    cursor = conn.cursor()
    
    print("✅ Connected to PostgreSQL")
    
    # Check if schema exists
    cursor.execute("SELECT schema_name FROM information_schema.schemata WHERE schema_name = 'crm_data';")
    if cursor.fetchone():
        print("✅ crm_data schema exists")
    else:
        print("❌ crm_data schema doesn't exist")
    
    # Check if tables exist
    cursor.execute("""
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'crm_data'
        ORDER BY table_name;
    """)
    tables = cursor.fetchall()
    print(f"Tables in crm_data: {[t[0] for t in tables]}")
    
    # Check row counts
    for table in ['accounts', 'addresses', 'devices']:
        cursor.execute(f"SELECT COUNT(*) FROM crm_data.{table};")
        count = cursor.fetchone()[0]
        print(f"  {table}: {count} rows")
    
    cursor.close()
    conn.close()
    
except Exception as e:
    print(f"❌ Error: {e}")
