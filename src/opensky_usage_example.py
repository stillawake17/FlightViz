#!/usr/bin/env python3
"""
OpenSky API Usage Example for Bristol Airport
This script shows how to use your credentials.json file to collect flight data
"""

from opensky_bristol_collector import OpenSkyBristolCollector, compare_with_aviationstack
from datetime import datetime, timedelta
import json
import os

def main():
    print("=== OpenSky Network Data Collector for Bristol Airport ===\n")
    
    # Check if credentials file exists
    credentials_file = "credentials.json"
    if not os.path.exists(credentials_file):
        print(f"❌ Credentials file '{credentials_file}' not found!")
        print("Make sure your credentials.json file is in the same directory.")
        print("It should look like:")
        print("""{
    "clientId": "your_username",
    "clientSecret": "your_client_secret"
}""")
        return
    
    # Initialize collector with credentials
    print("🔧 Initializing OpenSky collector with credentials...")
    collector = OpenSkyBristolCollector(credentials_file=credentials_file)
    
    # Test API connection
    print("\n🧪 Testing API connection...")
    connection_status = collector.test_api_connection()
    
    if not connection_status.get('connected'):
        print("❌ Could not connect to OpenSky API. Please check your credentials.")
        return
    
    # Get current API status
    print("\n📊 Checking API status...")
    api_status = collector.get_api_status()
    if api_status.get('rate_limit_remaining'):
        print(f"Remaining API calls today: {api_status['rate_limit_remaining']}")
    
    # Menu for different operations
    while True:
        print("\n" + "="*50)
        print("What would you like to do?")
        print("1. Collect current traffic around Bristol Airport")
        print("2. Collect historical data for a specific date")
        print("3. Compare with AviationStack data")
        print("4. Show API status")
        print("5. Exit")
        
        choice = input("\nEnter your choice (1-5): ").strip()
        
        if choice == '1':
            collect_current_traffic(collector)
        elif choice == '2':
            collect_historical_data(collector)
        elif choice == '3':
            compare_data(collector)
        elif choice == '4':
            show_api_status(collector)
        elif choice == '5':
            print("👋 Goodbye!")
            break
        else:
            print("❌ Invalid choice. Please try again.")

def collect_current_traffic(collector):
    """Collect current aircraft traffic around Bristol Airport"""
    print("\n🛩️  Collecting current traffic around Bristol Airport...")
    
    try:
        current_data = collector.collect_current_snapshot()
        
        if current_data and current_data.get('aircraft_count', 0) > 0:
            print(f"\n✅ Found {current_data['aircraft_count']} aircraft near Bristol Airport:")
            
            # Show details of nearby aircraft
            for i, aircraft in enumerate(current_data['aircraft'][:10], 1):
                callsign = aircraft.get('callsign', 'Unknown').strip() or f"Aircraft-{aircraft.get('icao24', 'Unknown')}"
                distance = aircraft.get('distance_from_bristol_km', 0)
                altitude = aircraft.get('baro_altitude') or aircraft.get('geo_altitude', 'Unknown')
                velocity = aircraft.get('velocity', 'Unknown')
                on_ground = aircraft.get('on_ground', False)
                
                status = "🛬 On Ground" if on_ground else f"✈️  Airborne ({altitude}m)"
                
                print(f"  {i:2d}. {callsign:12s} | {distance:5.1f}km away | {status} | {velocity} m/s")
            
            if len(current_data['aircraft']) > 10:
                print(f"     ... and {len(current_data['aircraft']) - 10} more aircraft")
                
        else:
            print("ℹ️  No aircraft currently detected near Bristol Airport")
            
    except Exception as e:
        print(f"❌ Error collecting current traffic: {e}")

def collect_historical_data(collector):
    """Collect historical flight data for a specific date"""
    print("\n📅 Historical Data Collection")
    
    # Get date from user
    while True:
        date_input = input("Enter date (YYYY-MM-DD) or 'yesterday' for yesterday: ").strip().lower()
        
        if date_input == 'yesterday':
            target_date = datetime.now() - timedelta(days=1)
            break
        else:
            try:
                target_date = datetime.strptime(date_input, '%Y-%m-%d')
                break
            except ValueError:
                print("❌ Invalid date format. Please use YYYY-MM-DD")
    
    print(f"\n📊 Collecting historical data for {target_date.strftime('%Y-%m-%d')}...")
    
    try:
        historical_data = collector.collect_daily_comparison(target_date)
        
        if historical_data:
            summary = historical_data.get('summary', {})
            print(f"\n✅ Data collected successfully:")
            print(f"   Total flights: {summary.get('total_flights', 0)}")
            print(f"   Arrivals: {summary.get('arrivals_count', 0)}")
            print(f"   Departures: {summary.get('departures_count', 0)}")
            
            # Show some sample flights
            arrivals = historical_data.get('arrivals', [])
            if arrivals:
                print(f"\n🛬 Sample arrivals:")
                for flight in arrivals[:5]:
                    callsign = flight.get('callsign', 'Unknown').strip() or 'Unknown'
                    country = flight.get('origin_country', 'Unknown')
                    print(f"   - {callsign} from {country}")
        else:
            print("❌ No historical data available for this date")
            
    except Exception as e:
        print(f"❌ Error collecting historical data: {e}")

def compare_data(collector):
    """Compare OpenSky data with AviationStack data"""
    print("\n🔄 Data Comparison")
    
    # List available files
    opensky_files = [f for f in os.listdir('.') if f.startswith('opensky_bristol_') and f.endswith('.json')]
    aviationstack_files = [f for f in os.listdir('.') if f.startswith('EGGD_') and f.endswith('.json')]
    
    if not opensky_files:
        print("❌ No OpenSky data files found. Collect some data first.")
        return
    
    if not aviationstack_files:
        print("❌ No AviationStack data files found.")
        print("Make sure you have AviationStack data files (EGGD_YYYY-MM-DD.json)")
        return
    
    print("📁 Available OpenSky files:")
    for i, file in enumerate(opensky_files, 1):
        print(f"   {i}. {file}")
    
    print("\n📁 Available AviationStack files:")
    for i, file in enumerate(aviationstack_files, 1):
        print(f"   {i}. {file}")
    
    # Simple comparison - you can enhance this
    if opensky_files and aviationstack_files:
        opensky_file = opensky_files[0]
        aviationstack_file = aviationstack_files[0]
        
        print(f"\n🔍 Comparing {opensky_file} with {aviationstack_file}...")
        
        try:
            with open(opensky_file, 'r') as f:
                opensky_data = json.load(f)
            
            compare_with_aviationstack(opensky_data, aviationstack_file)
            
        except Exception as e:
            print(f"❌ Error during comparison: {e}")

def show_api_status(collector):
    """Show current API status and rate limits"""
    print("\n📊 API Status Check")
    
    try:
        status = collector.get_api_status()
        
        print(f"Status: {status.get('status', 'Unknown')}")
        if status.get('rate_limit_remaining'):
            print(f"Rate limit remaining: {status['rate_limit_remaining']}")
        if status.get('response_time_ms'):
            print(f"Response time: {status['response_time_ms']:.1f}ms")
        if status.get('server_time'):
            print(f"Server time: {status['server_time']}")
            
    except Exception as e:
        print(f"❌ Error checking API status: {e}")

if __name__ == "__main__":
    main()