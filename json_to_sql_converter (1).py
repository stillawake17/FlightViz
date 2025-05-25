#!/usr/bin/env python3
"""
Convert JSON flight data to SQL INSERT statements for phpMyAdmin
"""

import json
import os
from datetime import datetime
from typing import Dict, Any

class JSONToSQLConverter:
    def __init__(self, data_directory: str = "data"):
        self.data_dir = data_directory
        
    def categorize_flight_time(self, time_str: str) -> str:
        """Categorize flight based on time"""
        if not time_str:
            return 'Unknown'
        
        try:
            dt = datetime.fromisoformat(time_str.replace('Z', '+00:00'))
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
    
    def escape_sql_string(self, value):
        """Escape strings for SQL"""
        if value is None:
            return 'NULL'
        if isinstance(value, str):
            # Escape single quotes and backslashes
            escaped = value.replace('\\', '\\\\').replace("'", "\\'")
            return f"'{escaped}'"
        if isinstance(value, bool):
            return '1' if value else '0'
        if isinstance(value, (int, float)):
            return str(value)
        return f"'{str(value)}'"
    
    def convert_datetime(self, time_str):
        """Convert datetime to MySQL format"""
        if not time_str:
            return 'NULL'
        try:
            dt = datetime.fromisoformat(time_str.replace('Z', '+00:00'))
            return f"'{dt.strftime('%Y-%m-%d %H:%M:%S')}'"
        except:
            return 'NULL'
    
    def process_single_flight(self, flight: Dict, flight_type: str, flight_date: str) -> str:
        """Convert a single flight to SQL INSERT statement"""
        try:
            # Check if flight is valid
            if not flight or not isinstance(flight, dict):
                return None
            
            # Extract basic flight info with safe defaults
            flight_info = flight.get('flight') or {}
            airline_info = flight.get('airline') or {}
            
            # Ensure we have dictionaries, not None
            if not isinstance(flight_info, dict):
                flight_info = {}
            if not isinstance(airline_info, dict):
                airline_info = {}
            
            flight_number = flight_info.get('number') or flight_info.get('iata', '')
            if not flight_number:
                return None  # Skip flights without numbers
            
            # Get timing info based on flight type with safe defaults
            if flight_type == 'arrival':
                time_info = flight.get('arrival') or {}
                other_info = flight.get('departure') or {}
            else:  # departure
                time_info = flight.get('departure') or {}
                other_info = flight.get('arrival') or {}
            
            # Ensure we have dictionaries
            if not isinstance(time_info, dict):
                time_info = {}
            if not isinstance(other_info, dict):
                other_info = {}
            
            # Extract airport information safely
            if flight_type == 'arrival':
                origin_airport = other_info.get('airport', '')
                origin_iata = other_info.get('iata', '')
                origin_icao = other_info.get('icao', '')
                destination_airport = time_info.get('airport', '')
                destination_iata = time_info.get('iata', '')
                destination_icao = time_info.get('icao', '')
            else:  # departure
                origin_airport = time_info.get('airport', '')
                origin_iata = time_info.get('iata', '')
                origin_icao = time_info.get('icao', '')
                destination_airport = other_info.get('airport', '')
                destination_iata = other_info.get('iata', '')
                destination_icao = other_info.get('icao', '')
            
            # Calculate delay
            delay_minutes = 'NULL'
            if time_info.get('scheduled') and time_info.get('actual'):
                try:
                    sched = datetime.fromisoformat(time_info['scheduled'].replace('Z', '+00:00'))
                    actual = datetime.fromisoformat(time_info['actual'].replace('Z', '+00:00'))
                    delay_minutes = str(int((actual - sched).total_seconds() / 60))
                except:
                    pass
            
            # Categorize time
            time_category = self.categorize_flight_time(time_info.get('actual'))
            
            # Handle codeshare with safe defaults
            codeshare_info = flight_info.get('codeshared') or {}
            if not isinstance(codeshare_info, dict):
                codeshare_info = {}
            is_codeshare = '1' if codeshare_info else '0'
            
            # Build SQL INSERT statement
            sql = f"""INSERT IGNORE INTO flights (
    flight_date, flight_number, airline_name, airline_iata, airline_icao,
    flight_type, flight_status,
    origin_airport, origin_iata, origin_icao,
    destination_airport, destination_iata, destination_icao,
    scheduled_utc, estimated_utc, actual_utc,
    terminal, gate, delay_minutes, time_category,
    is_codeshare, codeshare_airline_name, codeshare_airline_iata, codeshare_flight_number,
    raw_data
) VALUES (
    {self.escape_sql_string(flight_date)},
    {self.escape_sql_string(flight_number)},
    {self.escape_sql_string(airline_info.get('name', ''))},
    {self.escape_sql_string(airline_info.get('iata', ''))},
    {self.escape_sql_string(airline_info.get('icao', ''))},
    {self.escape_sql_string(flight_type)},
    {self.escape_sql_string(flight.get('flight_status', ''))},
    {self.escape_sql_string(origin_airport)},
    {self.escape_sql_string(origin_iata)},
    {self.escape_sql_string(origin_icao)},
    {self.escape_sql_string(destination_airport)},
    {self.escape_sql_string(destination_iata)},
    {self.escape_sql_string(destination_icao)},
    {self.convert_datetime(time_info.get('scheduled'))},
    {self.convert_datetime(time_info.get('estimated'))},
    {self.convert_datetime(time_info.get('actual'))},
    {self.escape_sql_string(time_info.get('terminal'))},
    {self.escape_sql_string(time_info.get('gate'))},
    {delay_minutes},
    {self.escape_sql_string(time_category)},
    {is_codeshare},
    {self.escape_sql_string(codeshare_info.get('airline_name', ''))},
    {self.escape_sql_string(codeshare_info.get('airline_iata', ''))},
    {self.escape_sql_string(codeshare_info.get('flight_number', ''))},
    {self.escape_sql_string(json.dumps(flight))}
);"""
            
            return sql
            
        except Exception as e:
            flight_num = "Unknown"
            try:
                if flight and isinstance(flight, dict):
                    flight_info = flight.get('flight') or {}
                    if isinstance(flight_info, dict):
                        flight_num = flight_info.get('number', 'Unknown')
            except:
                pass
            print(f"Error processing flight {flight_num}: {e}")
            return None
    
    def convert_json_files_to_sql(self, output_file: str = "flight_data_import.sql"):
        """Convert all JSON files to a single SQL file"""
        if not os.path.exists(self.data_dir):
            print(f"Directory {self.data_dir} does not exist")
            return
        
        sql_statements = []
        sql_statements.append("-- Bristol Airport Flight Data Import")
        sql_statements.append("-- Generated by JSON to SQL Converter")
        sql_statements.append("-- Run this in phpMyAdmin")
        sql_statements.append("")
        sql_statements.append("SET FOREIGN_KEY_CHECKS = 0;")
        sql_statements.append("SET AUTOCOMMIT = 0;")
        sql_statements.append("START TRANSACTION;")
        sql_statements.append("")
        
        total_flights = 0
        processed_files = 0
        
        for filename in sorted(os.listdir(self.data_dir)):
            if filename.endswith('.json') and not filename.startswith('index') and 'month_' not in filename:
                filepath = os.path.join(self.data_dir, filename)
                print(f"Processing {filename}...")
                
                try:
                    with open(filepath, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                    
                    flight_date = data.get('date')
                    if not flight_date:
                        print(f"  Skipping {filename} - no date found")
                        continue
                    
                    sql_statements.append(f"-- Data for {flight_date} from {filename}")
                    
                    # Process arrivals
                    arrivals = data.get('arrivals', [])
                    if arrivals and isinstance(arrivals, list):
                        for flight in arrivals:
                            if flight:  # Skip None entries
                                sql = self.process_single_flight(flight, 'arrival', flight_date)
                                if sql:
                                    sql_statements.append(sql)
                                    total_flights += 1
                    
                    # Process departures
                    departures = data.get('departures', [])
                    if departures and isinstance(departures, list):
                        for flight in departures:
                            if flight:  # Skip None entries
                                sql = self.process_single_flight(flight, 'departure', flight_date)
                                if sql:
                                    sql_statements.append(sql)
                                    total_flights += 1
                    
                    sql_statements.append("")
                    processed_files += 1
                    print(f"  ✅ Processed {len(arrivals)} arrivals, {len(departures)} departures")
                    
                except Exception as e:
                    print(f"  ❌ Error processing {filename}: {e}")
        
        # Add transaction commit
        sql_statements.append("COMMIT;")
        sql_statements.append("SET FOREIGN_KEY_CHECKS = 1;")
        sql_statements.append("SET AUTOCOMMIT = 1;")
        sql_statements.append("")
        sql_statements.append(f"-- Import completed: {total_flights} flights from {processed_files} files")
        sql_statements.append("SELECT 'Data import completed successfully!' as status;")
        
        # Write SQL file
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write('\n'.join(sql_statements))
        
        print(f"\n✅ SQL file created: {output_file}")
        print(f"📊 Total flights: {total_flights}")
        print(f"📁 Files processed: {processed_files}")
        print(f"\n📋 Next steps:")
        print(f"1. Open phpMyAdmin in your IONOS control panel")
        print(f"2. Go to your database: dbs14149844")  
        print(f"3. Click 'Import' tab")
        print(f"4. Choose file: {output_file}")
        print(f"5. Click 'Go' to import")

if __name__ == "__main__":
    converter = JSONToSQLConverter("data")
    converter.convert_json_files_to_sql("flight_data_import.sql")
