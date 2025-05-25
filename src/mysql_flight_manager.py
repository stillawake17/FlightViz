#!/usr/bin/env python3
"""
Flight Database Manager - MySQL/MariaDB implementation for IONOS hosting
"""

import mysql.connector
import json
import os
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
import logging

class MySQLFlightDatabaseManager:
    def __init__(self, host: str, database: str, user: str, password: str, port: int = 3306):
        self.config = {
            'host': host,
            'database': database,
            'user': user,
            'password': password,
            'port': port,
            'charset': 'utf8mb4',
            'collation': 'utf8mb4_unicode_ci',
            'autocommit': True,
            'raise_on_warnings': True
        }
        self.setup_database()
        
    def get_connection(self):
        """Get database connection"""
        return mysql.connector.connect(**self.config)
        
    def setup_database(self):
        """Create database tables if they don't exist"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            # Main flights table
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS flights (
                id INT AUTO_INCREMENT PRIMARY KEY,
                flight_date DATE NOT NULL,
                flight_number VARCHAR(20),
                airline_name VARCHAR(100),
                airline_iata VARCHAR(3),
                airline_icao VARCHAR(4),
                flight_type ENUM('arrival', 'departure') NOT NULL,
                flight_status VARCHAR(20),
                
                -- Airport information
                origin_airport VARCHAR(100),
                origin_iata VARCHAR(3),
                origin_icao VARCHAR(4),
                destination_airport VARCHAR(100),
                destination_iata VARCHAR(3),
                destination_icao VARCHAR(4),
                
                -- Timing information
                scheduled_utc DATETIME,
                estimated_utc DATETIME,
                actual_utc DATETIME,
                
                -- Additional details
                terminal VARCHAR(10),
                gate VARCHAR(10),
                delay_minutes INT,
                time_category VARCHAR(30),
                
                -- Codeshare information
                is_codeshare BOOLEAN DEFAULT FALSE,
                codeshare_airline_name VARCHAR(100),
                codeshare_airline_iata VARCHAR(3),
                codeshare_flight_number VARCHAR(20),
                
                -- Raw JSON data (for backup)
                raw_data JSON,
                
                -- Metadata
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                
                -- Indexes
                INDEX idx_flight_date (flight_date),
                INDEX idx_flight_type (flight_type),
                INDEX idx_airline (airline_iata),
                INDEX idx_time_category (time_category),
                INDEX idx_actual_time (actual_utc),
                
                -- Unique constraint to prevent duplicates
                UNIQUE KEY unique_flight (flight_date, flight_number, flight_type, scheduled_utc)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            ''')
            
            # Create summary table for quick stats
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS daily_summary (
                id INT AUTO_INCREMENT PRIMARY KEY,
                flight_date DATE NOT NULL UNIQUE,
                total_flights INT DEFAULT 0,
                total_arrivals INT DEFAULT 0,
                total_departures INT DEFAULT 0,
                night_flights INT DEFAULT 0,
                shoulder_flights INT DEFAULT 0,
                regular_flights INT DEFAULT 0,
                average_delay_minutes DECIMAL(10,2) DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                
                INDEX idx_date (flight_date)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            ''')
            
            conn.commit()
            print(f"Database tables created/verified successfully")
            
        finally:
            cursor.close()
            conn.close()
    
    def categorize_flight_time(self, time_str: str) -> str:
        """Categorize flight based on time"""
        if not time_str:
            return 'Unknown'
        
        try:
            # Convert to datetime and extract hour/minute
            if isinstance(time_str, str):
                dt = datetime.fromisoformat(time_str.replace('Z', '+00:00'))
            else:
                dt = time_str
                
            hour = dt.hour
            minute = dt.minute
            
            if hour == 23 and minute < 30:
                return 'Shoulder hour flights'
            elif (hour == 23 and minute >= 30) or hour < 6:
                return 'Night hour flights'
            elif hour == 6:
                return 'Shoulder hour flights'
            else:
                return 'Regular flights'
        except:
            return 'Unknown'
    
    def insert_flight_data(self, data: Dict[str, Any]) -> bool:
        """Insert flight data from your JSON format"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            flight_date = data.get('date')
            arrivals = data.get('arrivals', [])
            departures = data.get('departures', [])
            
            flights_inserted = 0
            
            # Process arrivals
            for flight in arrivals:
                if self._insert_single_flight(cursor, flight, 'arrival', flight_date):
                    flights_inserted += 1
            
            # Process departures
            for flight in departures:
                if self._insert_single_flight(cursor, flight, 'departure', flight_date):
                    flights_inserted += 1
            
            # Update daily summary
            self._update_daily_summary(cursor, flight_date)
            
            conn.commit()
            print(f"Inserted {flights_inserted} flights for {flight_date}")
            return True
            
        except Exception as e:
            print(f"Error inserting flight data: {e}")
            conn.rollback()
            return False
        finally:
            cursor.close()
            conn.close()
    
    def _insert_single_flight(self, cursor, flight: Dict, flight_type: str, flight_date: str) -> bool:
        """Insert a single flight record"""
        try:
            # Extract basic flight info
            flight_info = flight.get('flight', {})
            airline_info = flight.get('airline', {})
            
            flight_number = flight_info.get('number') or flight_info.get('iata', '')
            if not flight_number:
                return False
            
            # Get timing info based on flight type
            if flight_type == 'arrival':
                time_info = flight.get('arrival', {})
                other_info = flight.get('departure', {})
                origin_airport = other_info.get('airport', '')
                origin_iata = other_info.get('iata', '')
                origin_icao = other_info.get('icao', '')
                destination_airport = time_info.get('airport', '')
                destination_iata = time_info.get('iata', '')
                destination_icao = time_info.get('icao', '')
            else:  # departure
                time_info = flight.get('departure', {})
                other_info = flight.get('arrival', {})
                origin_airport = time_info.get('airport', '')
                origin_iata = time_info.get('iata', '')
                origin_icao = time_info.get('icao', '')
                destination_airport = other_info.get('airport', '')
                destination_iata = other_info.get('iata', '')
                destination_icao = other_info.get('icao', '')
            
            # Calculate delay
            delay_minutes = None
            if time_info.get('scheduled') and time_info.get('actual'):
                try:
                    sched = datetime.fromisoformat(time_info['scheduled'].replace('Z', '+00:00'))
                    actual = datetime.fromisoformat(time_info['actual'].replace('Z', '+00:00'))
                    delay_minutes = int((actual - sched).total_seconds() / 60)
                except:
                    pass
            
            # Categorize time
            time_category = self.categorize_flight_time(time_info.get('actual'))
            
            # Handle codeshare
            codeshare_info = flight_info.get('codeshared', {})
            is_codeshare = bool(codeshare_info)
            
            # Convert times to proper format
            def convert_time(time_str):
                if not time_str:
                    return None
                try:
                    return datetime.fromisoformat(time_str.replace('Z', '+00:00'))
                except:
                    return None
            
            # Insert the record
            cursor.execute('''
            INSERT INTO flights (
                flight_date, flight_number, airline_name, airline_iata, airline_icao,
                flight_type, flight_status,
                origin_airport, origin_iata, origin_icao,
                destination_airport, destination_iata, destination_icao,
                scheduled_utc, estimated_utc, actual_utc,
                terminal, gate, delay_minutes, time_category,
                is_codeshare, codeshare_airline_name, codeshare_airline_iata, codeshare_flight_number,
                raw_data
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            ) ON DUPLICATE KEY UPDATE
                flight_status = VALUES(flight_status),
                estimated_utc = VALUES(estimated_utc),
                actual_utc = VALUES(actual_utc),
                delay_minutes = VALUES(delay_minutes),
                time_category = VALUES(time_category),
                updated_at = CURRENT_TIMESTAMP
            ''', (
                flight_date,
                flight_number,
                airline_info.get('name', ''),
                airline_info.get('iata', ''),
                airline_info.get('icao', ''),
                flight_type,
                flight.get('flight_status', ''),
                origin_airport,
                origin_iata,
                origin_icao,
                destination_airport,
                destination_iata,
                destination_icao,
                convert_time(time_info.get('scheduled')),
                convert_time(time_info.get('estimated')),
                convert_time(time_info.get('actual')),
                time_info.get('terminal'),
                time_info.get('gate'),
                delay_minutes,
                time_category,
                is_codeshare,
                codeshare_info.get('airline_name', ''),
                codeshare_info.get('airline_iata', ''),
                codeshare_info.get('flight_number', ''),
                json.dumps(flight)  # Store raw data as JSON
            ))
            
            return True
            
        except Exception as e:
            print(f"Error inserting single flight {flight_number}: {e}")
            return False
    
    def _update_daily_summary(self, cursor, flight_date: str):
        """Update daily summary statistics"""
        cursor.execute('''
        INSERT INTO daily_summary (
            flight_date, total_flights, total_arrivals, total_departures,
            night_flights, shoulder_flights, regular_flights, average_delay_minutes
        )
        SELECT 
            %s as flight_date,
            COUNT(*) as total_flights,
            SUM(CASE WHEN flight_type = 'arrival' THEN 1 ELSE 0 END) as total_arrivals,
            SUM(CASE WHEN flight_type = 'departure' THEN 1 ELSE 0 END) as total_departures,
            SUM(CASE WHEN time_category = 'Night hour flights' THEN 1 ELSE 0 END) as night_flights,
            SUM(CASE WHEN time_category = 'Shoulder hour flights' THEN 1 ELSE 0 END) as shoulder_flights,
            SUM(CASE WHEN time_category = 'Regular flights' THEN 1 ELSE 0 END) as regular_flights,
            AVG(COALESCE(delay_minutes, 0)) as average_delay_minutes
        FROM flights 
        WHERE flight_date = %s
        ON DUPLICATE KEY UPDATE
            total_flights = VALUES(total_flights),
            total_arrivals = VALUES(total_arrivals),
            total_departures = VALUES(total_departures),
            night_flights = VALUES(night_flights),
            shoulder_flights = VALUES(shoulder_flights),
            regular_flights = VALUES(regular_flights),
            average_delay_minutes = VALUES(average_delay_minutes),
            updated_at = CURRENT_TIMESTAMP
        ''', (flight_date, flight_date))
    
    def import_json_files(self, data_directory: str = "data") -> int:
        """Import all JSON files from directory"""
        imported_count = 0
        
        if not os.path.exists(data_directory):
            print(f"Directory {data_directory} does not exist")
            return 0
        
        for filename in os.listdir(data_directory):
            if filename.endswith('.json') and not filename.startswith('index') and 'month_' not in filename:
                filepath = os.path.join(data_directory, filename)
                print(f"Importing {filename}...")
                
                try:
                    with open(filepath, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                    
                    if self.insert_flight_data(data):
                        imported_count += 1
                        print(f"✅ Successfully imported {filename}")
                    else:
                        print(f"❌ Failed to import {filename}")
                        
                except Exception as e:
                    print(f"❌ Error reading {filename}: {e}")
        
        print(f"\nImported {imported_count} files into database")
        return imported_count
    
    def get_flight_stats(self) -> Dict[str, Any]:
        """Get basic statistics about flights in database"""
        conn = self.get_connection()
        cursor = conn.cursor(dictionary=True)
        
        try:
            # Total flights
            cursor.execute("SELECT COUNT(*) as total FROM flights")
            total_flights = cursor.fetchone()['total']
            
            # By type
            cursor.execute("SELECT flight_type, COUNT(*) as count FROM flights GROUP BY flight_type")
            by_type = {row['flight_type']: row['count'] for row in cursor.fetchall()}
            
            # By time category
            cursor.execute("SELECT time_category, COUNT(*) as count FROM flights GROUP BY time_category")
            by_category = {row['time_category']: row['count'] for row in cursor.fetchall()}
            
            # Date range
            cursor.execute("SELECT MIN(flight_date) as min_date, MAX(flight_date) as max_date FROM flights")
            date_range = cursor.fetchone()
            
            # Top airlines
            cursor.execute("""
                SELECT airline_name, COUNT(*) as count 
                FROM flights 
                WHERE airline_name != '' 
                GROUP BY airline_name 
                ORDER BY count DESC 
                LIMIT 10
            """)
            top_airlines = [(row['airline_name'], row['count']) for row in cursor.fetchall()]
            
            return {
                'total_flights': total_flights,
                'by_type': by_type,
                'by_category': by_category,
                'date_range': (date_range['min_date'], date_range['max_date']),
                'top_airlines': top_airlines
            }
            
        finally:
            cursor.close()
            conn.close()
    
    def export_for_web(self, start_date: str, end_date: str, output_file: str = None) -> Dict[str, Any]:
        """Export data in format compatible with your HTML dashboard"""
        conn = self.get_connection()
        cursor = conn.cursor(dictionary=True)
        
        try:
            # Get flights for date range
            cursor.execute("""
                SELECT * FROM flights 
                WHERE flight_date BETWEEN %s AND %s
                ORDER BY flight_date, actual_utc
            """, (start_date, end_date))
            
            flights = cursor.fetchall()
            
            # Group by date and type
            result = {}
            
            for flight in flights:
                date_str = flight['flight_date'].strftime('%Y-%m-%d')
                
                if date_str not in result:
                    result[date_str] = {'arrivals': [], 'departures': []}
                
                # Convert back to your original format
                flight_data = {
                    'flight_date': date_str,
                    'flight_status': flight['flight_status'],
                    'flight': {
                        'number': flight['flight_number'],
                        'iata': flight['flight_number'],
                        'codeshared': {
                            'airline_name': flight['codeshare_airline_name'],
                            'flight_number': flight['codeshare_flight_number']
                        } if flight['is_codeshare'] else None
                    },
                    'airline': {
                        'name': flight['airline_name'],
                        'iata': flight['airline_iata'],
                        'icao': flight['airline_icao']
                    },
                    'time_category': flight['time_category']
                }
                
                # Add arrival/departure specific info
                if flight['flight_type'] == 'arrival':
                    flight_data['arrival'] = {
                        'airport': flight['destination_airport'],
                        'iata': flight['destination_iata'],
                        'icao': flight['destination_icao'],
                        'scheduled': flight['scheduled_utc'].isoformat() if flight['scheduled_utc'] else None,
                        'actual': flight['actual_utc'].isoformat() if flight['actual_utc'] else None,
                        'terminal': flight['terminal'],
                        'gate': flight['gate']
                    }
                    flight_data['departure'] = {
                        'airport': flight['origin_airport'],
                        'iata': flight['origin_iata'],
                        'icao': flight['origin_icao']
                    }
                    result[date_str]['arrivals'].append(flight_data)
                else:
                    flight_data['departure'] = {
                        'airport': flight['origin_airport'],
                        'iata': flight['origin_iata'],
                        'icao': flight['origin_icao'],
                        'scheduled': flight['scheduled_utc'].isoformat() if flight['scheduled_utc'] else None,
                        'actual': flight['actual_utc'].isoformat() if flight['actual_utc'] else None,
                        'terminal': flight['terminal'],
                        'gate': flight['gate']
                    }
                    flight_data['arrival'] = {
                        'airport': flight['destination_airport'],
                        'iata': flight['destination_iata'],
                        'icao': flight['destination_icao']
                    }
                    result[date_str]['departures'].append(flight_data)
            
            # Convert to array format for your dashboard
            dashboard_data = []
            for date, flights in sorted(result.items()):
                dashboard_data.append({
                    'date': date,
                    'data': flights
                })
            
            # Optionally save to file
            if output_file:
                with open(output_file, 'w', encoding='utf-8') as f:
                    json.dump(dashboard_data, f, indent=2, ensure_ascii=False)
                print(f"Exported data to {output_file}")
            
            return dashboard_data
            
        finally:
            cursor.close()
            conn.close()


# Usage example and configuration
if __name__ == "__main__":
    # IONOS database configuration - UPDATE THESE WITH YOUR DETAILS
    DB_CONFIG = {
        'host': 'db5017700588.hosting-data.io',  # e.g., 'db5013579021.hosting-data.io'
        'database': 'dbs14149844',        # e.g., 'db13579021'
        'user': 'dbu4392139',                 # e.g., 'dbo13579021'
        'password': 'wJFtEZ4Ww?8!vCt',             # Your database password
        'port': 3306
    }
    
    print("=== MySQL Flight Database Manager ===")
    print("Update DB_CONFIG with your IONOS database details before running!")
    
    # Uncomment and configure these lines when ready:
    db = MySQLFlightDatabaseManager(**DB_CONFIG)
    imported = db.import_json_files("data")
    stats = db.get_flight_stats()
    print(f"Database now contains {stats['total_flights']} flights")
