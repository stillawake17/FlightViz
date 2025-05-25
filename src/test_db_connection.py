#!/usr/bin/env python3
"""
Simple script to test IONOS database connection
"""

import mysql.connector
from mysql.connector import Error

# Try different possible host formats for IONOS
HOST_OPTIONS = [
    'mysql5017700588.db.hosting-data.io',  # Most common IONOS format
    'db5017700588.hosting-data.io',        # Your original
    'db5017700588.perfora.net',            # Alternative IONOS format
    'mysql5017700588.perfora.net',         # Another alternative
]

# YOUR ACTUAL IONOS CREDENTIALS:
DATABASE = 'dbs14149844'
USERNAME = 'dbu4392139'  
PASSWORD = 'wJFtEZ4Ww?8!vCt'  # Put your actual password here
PORT = 3306

def test_connection(host):
    """Test connection to database"""
    try:
        print(f"Testing connection to: {host}")
        
        connection = mysql.connector.connect(
            host=host,
            database=DATABASE,
            user=USERNAME,
            password=PASSWORD,
            port=PORT,
            connection_timeout=10
        )
        
        if connection.is_connected():
            cursor = connection.cursor()
            cursor.execute("SELECT VERSION()")
            version = cursor.fetchone()
            print(f"✅ SUCCESS! Connected to MySQL version: {version[0]}")
            print(f"✅ Working host: {host}")
            
            # Test database access
            cursor.execute(f"USE {DATABASE}")
            cursor.execute("SHOW TABLES")
            tables = cursor.fetchall()
            print(f"✅ Database '{DATABASE}' accessible")
            print(f"   Current tables: {[table[0] for table in tables]}")
            
            return True
            
    except Error as e:
        print(f"❌ Failed to connect to {host}")
        print(f"   Error: {e}")
        return False
        
    finally:
        if 'connection' in locals() and connection.is_connected():
            cursor.close()
            connection.close()

if __name__ == "__main__":
    print("=== IONOS Database Connection Test ===")
    print(f"Database: {DATABASE}")
    print(f"Username: {USERNAME}")
    print(f"Port: {PORT}")
    print()
    
    # Test each host option
    for host in HOST_OPTIONS:
        if test_connection(host):
            print(f"\n🎉 Use this host in your DB_CONFIG: '{host}'")
            break
        print()
    else:
        print("\n❌ None of the host formats worked.")
        print("\nPlease check:")
        print("1. Your IONOS control panel for exact host name")
        print("2. Database name, username, and password are correct")
        print("3. Database is active and running")
        print("4. Your internet connection")
