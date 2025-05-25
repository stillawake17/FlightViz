#!/usr/bin/env python3
"""
Convert daily flight data to SQL for phpMyAdmin import
Designed for daily updates after running your data collection script
"""

import json
import os
from datetime import datetime
from typing import Dict, Any

class DailyUpdateConverter:
    def __init__(self):
        pass
        
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
            if not flight or not isinstance(flight, dict):
                return None
            
            flight_info = flight.get('flight') or {}
            airline_info = flight.get('airline') or {}
            
            if not isinstance(flight_info, dict):
                flight_info = {}
            if not isinstance(airline_info, dict):
                airline_info = {}
            
            flight_number = flight_info.get('number') or flight_info.get('iata', '')
            if not flight_number:
                return None
            
            # Get timing info based on flight type with safe defaults
            if flight_type == 'arrival':
                time_info = flight.get('arrival') or {}
                other_info = flight.get('departure') or {}
            else:
                time_info = flight.get('departure') or {}
                other_info = flight.get('arrival') or {}
            
            if not isinstance(time_info, dict):
                time_info = {}
            if not isinstance(other_info, dict):
                other_info = {}
            
            # Extract airport information
            if flight_type == 'arrival':
                origin_airport = other_info.get('airport', '')
                origin_iata = other_info.get('iata', '')
                origin_icao = other_info.get('icao', '')
                destination_airport = time_info.get('airport', '')
                destination_iata = time_info.get('iata', '')
                destination_icao = time_info.get('icao', '')
            else:
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
            
            time_category = self.categorize_flight_time(time_info.get('actual'))
            
            codeshare_info = flight_info.get('codeshared') or {}
            if not isinstance(codeshare_info, dict):
                codeshare_info = {}
            is_codeshare = '1' if codeshare_info else '0'
            
            # Use INSERT ... ON DUPLICATE KEY UPDATE for daily updates
            sql = f"""INSERT INTO flights (
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
) ON DUPLICATE KEY UPDATE
    flight_status = VALUES(flight_status),
    estimated_utc = VALUES(estimated_utc),
    actual_utc = VALUES(actual_utc),
    delay_minutes = VALUES(delay_minutes),
    time_category = VALUES(time_category),
    updated_at = CURRENT_TIMESTAMP;"""
            
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
    
    def convert_daily_file(self, json_file_path: str, output_file: str = None):
        """Convert a single daily JSON file to SQL"""
        if not os.path.exists(json_file_path):
            print(f"File not found: {json_file_path}")
            return
        
        if not output_file:
            # Create output filename based on input
            base_name = os.path.splitext(os.path.basename(json_file_path))[0]
            output_file = f"daily_update_{base_name}.sql"
        
        try:
            with open(json_file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
        except Exception as e:
            print(f"Error reading JSON file: {e}")
            return
        
        flight_date = data.get('date')
        if not flight_date:
            print("No date found in JSON file")
            return
        
        sql_statements = []
        sql_statements.append(f"-- Daily flight data update for {flight_date}")
        sql_statements.append(f"-- Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        sql_statements.append("")
        sql_statements.append("SET AUTOCOMMIT = 0;")
        sql_statements.append("START TRANSACTION;")
        sql_statements.append("")
        
        # First, delete existing data for this date (in case of re-runs)
        sql_statements.append(f"-- Remove existing data for {flight_date}")
        sql_statements.append(f"DELETE FROM flights WHERE flight_date = '{flight_date}';")
        sql_statements.append("")
        
        total_flights = 0
        
        # Process arrivals
        arrivals = data.get('arrivals', [])
        if arrivals and isinstance(arrivals, list):
            sql_statements.append(f"-- Arrivals for {flight_date}")
            for flight in arrivals:
                if flight:
                    sql = self.process_single_flight(flight, 'arrival', flight_date)
                    if sql:
                        sql_statements.append(sql)
                        total_flights += 1
            sql_statements.append("")
        
        # Process departures
        departures = data.get('departures', [])
        if departures and isinstance(departures, list):
            sql_statements.append(f"-- Departures for {flight_date}")
            for flight in departures:
                if flight:
                    sql = self.process_single_flight(flight, 'departure', flight_date)
                    if sql:
                        sql_statements.append(sql)
                        total_flights += 1
            sql_statements.append("")
        
        # Update daily summary
        sql_statements.append(f"-- Update daily summary for {flight_date}")
        sql_statements.append(f"""INSERT INTO daily_summary (
    flight_date, total_flights, total_arrivals, total_departures,
    night_flights, shoulder_flights, regular_flights, average_delay_minutes
)
SELECT 
    '{flight_date}' as flight_date,
    COUNT(*) as total_flights,
    SUM(CASE WHEN flight_type = 'arrival' THEN 1 ELSE 0 END) as total_arrivals,
    SUM(CASE WHEN flight_type = 'departure' THEN 1 ELSE 0 END) as total_departures,
    SUM(CASE WHEN time_category = 'Night hour flights' THEN 1 ELSE 0 END) as night_flights,
    SUM(CASE WHEN time_category = 'Shoulder hour flights' THEN 1 ELSE 0 END) as shoulder_flights,
    SUM(CASE WHEN time_category = 'Regular flights' THEN 1 ELSE 0 END) as regular_flights,
    AVG(COALESCE(delay_minutes, 0)) as average_delay_minutes
FROM flights 
WHERE flight_date = '{flight_date}'
ON DUPLICATE KEY UPDATE
    total_flights = VALUES(total_flights),
    total_arrivals = VALUES(total_arrivals),
    total_departures = VALUES(total_departures),
    night_flights = VALUES(night_flights),
    shoulder_flights = VALUES(shoulder_flights),
    regular_flights = VALUES(regular_flights),
    average_delay_minutes = VALUES(average_delay_minutes),
    updated_at = CURRENT_TIMESTAMP;""")
        
        sql_statements.append("")
        sql_statements.append("COMMIT;")
        sql_statements.append("SET AUTOCOMMIT = 1;")
        sql_statements.append("")
        sql_statements.append(f"-- Update completed: {total_flights} flights for {flight_date}")
        sql_statements.append("SELECT 'Daily update completed successfully!' as status;")
        
        # Write SQL file
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write('\n'.join(sql_statements))
        
        print(f"✅ Daily update SQL created: {output_file}")
        print(f"📊 Flights processed: {total_flights}")
        print(f"📅 Date: {flight_date}")
        print(f"\n📋 Next steps:")
        print(f"1. Open phpMyAdmin in your IONOS control panel")
        print(f"2. Go to your database: dbs14149844")
        print(f"3. Click 'Import' tab")
        print(f"4. Choose file: {output_file}")
        print(f"5. Click 'Go' to import")
        print(f"\n✨ This will update/replace data for {flight_date}")

if __name__ == "__main__":
    import sys
    
    converter = DailyUpdateConverter()
    
    if len(sys.argv) > 1:
        # File specified as command line argument
        json_file = sys.argv[1]
        converter.convert_daily_file(json_file)
    else:
        # Look for the most recent JSON file
        data_dir = "data"
        if os.path.exists(data_dir):
            json_files = [f for f in os.listdir(data_dir) 
                         if f.endswith('.json') and not f.startswith('index') and 'month_' not in f]
            
            if json_files:
                # Sort by modification time, get most recent
                json_files.sort(key=lambda x: os.path.getmtime(os.path.join(data_dir, x)), reverse=True)
                latest_file = os.path.join(data_dir, json_files[0])
                print(f"Processing most recent file: {latest_file}")
                converter.convert_daily_file(latest_file)
            else:
                print("No JSON files found in data directory")
                print("Usage: python daily_update_converter.py [path/to/json/file]")
        else:
            print("Data directory not found")
            print("Usage: python daily_update_converter.py [path/to/json/file]")
