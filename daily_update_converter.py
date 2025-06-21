#!/usr/bin/env python3
"""
Convert daily flight data to SQL for phpMyAdmin import
Designed for daily updates after running your data collection script
This version automatically selects the JSON file whose internal "date" field is exactly two days ago.
"""

import json
import os
from datetime import datetime, date, timedelta
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
            hour, minute = dt.hour, dt.minute
            if hour == 23 and minute < 30:
                return 'Shoulder hour flights'
            if (hour == 23 and minute >= 30) or hour < 6:
                return 'Night hour flights'
            if hour == 6:
                return 'Shoulder hour flights'
            return 'Regular flights'
        except:
            return 'Unknown'
    
    def escape_sql_string(self, value: Any) -> str:
        """Escape values for safe SQL insertion"""
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

    def convert_datetime(self, time_str: str) -> str:
        """Convert ISO timestamp to MySQL DATETIME string"""
        if not time_str:
            return 'NULL'
        try:
            dt = datetime.fromisoformat(time_str.replace('Z', '+00:00'))
            return f"'{dt.strftime('%Y-%m-%d %H:%M:%S')}'"
        except:
            return 'NULL'

    def process_single_flight(self, flight: Dict[str, Any], flight_type: str, flight_date: str) -> str:
        """Convert a single flight record to an SQL INSERT ... ON DUPLICATE KEY UPDATE statement"""
        try:
            if not flight or not isinstance(flight, dict):
                return None

            flight_info = flight.get('flight') or {}
            airline_info = flight.get('airline') or {}

            flight_number = flight_info.get('number') or flight_info.get('iata', '')
            if not flight_number:
                return None

            # Determine time_info and other_info based on flight_type
            if flight_type == 'arrival':
                time_info = flight.get('arrival') or {}
                other_info = flight.get('departure') or {}
            else:
                time_info = flight.get('departure') or {}
                other_info = flight.get('arrival') or {}

            # Assign origin/destination dicts
            origin = other_info if flight_type == 'arrival' else time_info
            dest = time_info if flight_type == 'arrival' else other_info

            origin_airport = origin.get('airport', '')
            origin_iata = origin.get('iata', '')
            origin_icao = origin.get('icao', '')
            dest_airport = dest.get('airport', '')
            dest_iata = dest.get('iata', '')
            dest_icao = dest.get('icao', '')

            # Compute delay
            delay_minutes = 'NULL'
            sched = time_info.get('scheduled')
            actual = time_info.get('actual')
            if sched and actual:
                try:
                    dt_sched = datetime.fromisoformat(sched.replace('Z', '+00:00'))
                    dt_actual = datetime.fromisoformat(actual.replace('Z', '+00:00'))
                    delay_minutes = str(int((dt_actual - dt_sched).total_seconds() // 60))
                except:
                    pass

            time_category = self.categorize_flight_time(actual)
            codeshare_info = flight_info.get('codeshared') or {}
            is_codeshare = '1' if codeshare_info else '0'

            # Build the INSERT statement
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
    {self.escape_sql_string(airline_info.get('name',''))},
    {self.escape_sql_string(airline_info.get('iata',''))},
    {self.escape_sql_string(airline_info.get('icao',''))},
    {self.escape_sql_string(flight_type)},
    {self.escape_sql_string(flight.get('flight_status',''))},
    {self.escape_sql_string(origin_airport)},
    {self.escape_sql_string(origin_iata)},
    {self.escape_sql_string(origin_icao)},
    {self.escape_sql_string(dest_airport)},
    {self.escape_sql_string(dest_iata)},
    {self.escape_sql_string(dest_icao)},
    {self.convert_datetime(sched)},
    {self.convert_datetime(time_info.get('estimated'))},
    {self.convert_datetime(actual)},
    {self.escape_sql_string(time_info.get('terminal'))},
    {self.escape_sql_string(time_info.get('gate'))},
    {delay_minutes},
    {self.escape_sql_string(time_category)},
    {is_codeshare},
    {self.escape_sql_string(codeshare_info.get('airline_name',''))},
    {self.escape_sql_string(codeshare_info.get('airline_iata',''))},
    {self.escape_sql_string(codeshare_info.get('flight_number',''))},
    {self.escape_sql_string(json.dumps(flight))}
) ON DUPLICATE KEY UPDATE
    flight_status=VALUES(flight_status),
    estimated_utc=VALUES(estimated_utc),
    actual_utc=VALUES(actual_utc),
    delay_minutes=VALUES(delay_minutes),
    time_category=VALUES(time_category),
    updated_at=CURRENT_TIMESTAMP;"""
            return sql

        except Exception as e:
            print(f"Error processing flight: {e}")
            return None

    def convert_daily_file(self, json_file_path: str, output_file: str = None):
        """Load the JSON, generate SQL, and write to an .sql file"""
        if not os.path.exists(json_file_path):
            print(f"File not found: {json_file_path}")
            return

        with open(json_file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)

        flight_date = data.get('date')
        if not flight_date:
            print("No date found in JSON file")
            return

        if not output_file:
            base = os.path.splitext(os.path.basename(json_file_path))[0]
            output_file = f"daily_update_{base}.sql"

        stmts = [
            f"-- Daily flight data update for {flight_date}",
            f"-- Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            "", "SET AUTOCOMMIT = 0;", "START TRANSACTION;", "",
            f"-- Remove existing data for {flight_date}",
            f"DELETE FROM flights WHERE flight_date='{flight_date}';", ""
        ]

        total = 0
        for segment, typ in ((data.get('arrivals', []), 'arrival'), (data.get('departures', []), 'departure')):
            if segment and isinstance(segment, list):
                stmts.append(f"-- {typ.capitalize()}s for {flight_date}")
                for fl in segment:
                    sql = self.process_single_flight(fl, typ, flight_date)
                    if sql:
                        stmts.append(sql)
                        total += 1
                stmts.append("")

        stmts.append(
            f"-- Update daily summary for {flight_date}\nINSERT INTO daily_summary (\n    flight_date, total_flights, total_arrivals, total_departures,\n    night_flights, shoulder_flights, regular_flights, average_delay_minutes\n) SELECT \n    '{flight_date}',\n    COUNT(*),\n    SUM(CASE WHEN flight_type='arrival' THEN 1 ELSE 0 END),\n    SUM(CASE WHEN flight_type='departure' THEN 1 ELSE 0 END),\n    SUM(CASE WHEN time_category='Night hour flights' THEN 1 ELSE 0 END),\n    SUM(CASE WHEN time_category='Shoulder hour flights' THEN 1 ELSE 0 END),\n    SUM(CASE WHEN time_category='Regular flights' THEN 1 ELSE 0 END),\n    AVG(COALESCE(delay_minutes,0))\nFROM flights WHERE flight_date='{flight_date}'\nON DUPLICATE KEY UPDATE\n    total_flights=VALUES(total_flights),\n    total_arrivals=VALUES(total_arrivals),\n    total_departures=VALUES(total_departures),\n    night_flights=VALUES(night_flights),\n    shoulder_flights=VALUES(shoulder_flights),\n    regular_flights=VALUES(regular_flights),\n    average_delay_minutes=VALUES(average_delay_minutes),\n    updated_at=CURRENT_TIMESTAMP;"
        )

        stmts.extend(["", "COMMIT;", "SET AUTOCOMMIT = 1;", "", f"-- Update completed: {total} flights for {flight_date}", "SELECT 'Daily update completed successfully!' AS status;"])

        with open(output_file, 'w', encoding='utf-8') as f:
            f.write('\n'.join(stmts))

        print(f"✅ Daily update SQL created: {output_file}")
        print(f"📊 Flights processed: {total}")
        print(f"📅 Date: {flight_date}")
        print("\n📋 Next steps:")
        print("1. Open phpMyAdmin in your IONOS control panel")
        print("2. Go to your database: dbs#####")
        print("3. Click 'Import' tab")
        print(f"4. Choose file: {output_file}")
        print("5. Click 'Go' to import")

        print(f"\n✨ This will update/replace data for {flight_date}")

if __name__ == "__main__":
    import sys
    converter = DailyUpdateConverter()
    data_dir = "data"

    # Compute the date two days ago
    target_date = date.today() - timedelta(days=1)
    target_str  = target_date.strftime("%Y-%m-%d")

    if len(sys.argv) > 1:
        converter.convert_daily_file(sys.argv[1])
    else:
        if os.path.isdir(data_dir):
            found = False
            for fname in os.listdir(data_dir):
                if not fname.endswith('.json'):
                    continue
                full_path = os.path.join(data_dir, fname)
                try:
                    with open(full_path, 'r', encoding='utf-8') as f:
                        info = json.load(f)
                except:
                    continue

                if info.get('date') == target_str:
                    print(f"Processing JSON dated {target_str}: {full_path}")
                    converter.convert_daily_file(full_path)
                    found = True
                    break
            if not found:
                print(f"❌ No JSON file in '{data_dir}' has a 'date' of {target_str}.")
                print("Ensure one of your data files contains a matching 'date' field.")
        else:
            print(f"Data directory not found: {data_dir}")
            print("Usage: python daily_update_converter.py [path/to/json/file]")
