import requests
import json
from datetime import datetime, timedelta
import csv
import os
import time
import random
import sys

from config import FR24_API_KEY
# Replace with your actual API token
API_TOKEN = FR24_API_KEY
BRISTOL_AIRPORT_CODE = 'BRS'
OUTPUT_DIR = 'bristol_flight_data'
DAYS_TO_ANALYZE = 7  # Number of days to go back
SINGLE_DAY_MODE = False  # Set to True to analyze just a single day
SINGLE_DAY_DATE = None  # Format: 'YYYY-MM-DD' (only used if SINGLE_DAY_MODE is True)
SKIP_CONFIRMATION = True  # Set to True to skip the confirmation prompt

# Create output directory if it doesn't exist
if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)

def create_auth_headers():
    """Create authentication headers for the API requests"""
    return {
        'Accept': 'application/json',
        'Accept-Version': 'v1',
        'Authorization': f'Bearer {API_TOKEN}'
    }

def fetch_flights_for_date(date, direction='both', max_retries=5):
    timestamp = int(date.replace(hour=12, minute=0, second=0).timestamp())
    
    if direction == 'both':
        airport_param = f'both:{BRISTOL_AIRPORT_CODE}'
    else:
        airport_param = f'{direction}:{BRISTOL_AIRPORT_CODE}'
    
    params = {
        'timestamp': timestamp,
        'airports': airport_param,
        'limit': 500
    }
    
    for attempt in range(max_retries):
        try:
            print(f"Fetching {direction} flights for {date.strftime('%Y-%m-%d')}... (Attempt {attempt+1}/{max_retries})")
            response = requests.get(
                'https://fr24api.flightradar24.com/api/historic/flight-positions/full',
                headers=create_auth_headers(),
                params=params
            )
            
            if response.status_code == 200:
                data = response.json()
                # Debug prints to inspect the response type and content
                print("DEBUG: Type of response:", type(data))
                print("DEBUG: Response content:", data)
                
                if isinstance(data, dict):
                    flights = data.get('data', [])
                elif isinstance(data, list):
                    flights = data
                else:
                    flights = []
                
                print(f"Retrieved {len(flights)} {direction} flights")
                return flights
                
            elif response.status_code == 429:
                retry_after = int(response.headers.get('Retry-After', 60))
                print(f"Rate limited. Waiting {retry_after} seconds before retry...")
                time.sleep(retry_after)
                continue
            else:
                print(f"Error fetching {direction} flights: {response.status_code}")
                print(response.text)
                if 500 <= response.status_code < 600:
                    wait_time = (attempt + 1) * 5
                    print(f"Server error. Retrying in {wait_time} seconds...")
                    time.sleep(wait_time)
                    continue
                else:
                    return []
                    
        except requests.exceptions.RequestException as e:
            print(f"Request exception: {e}")
            wait_time = (attempt + 1) * 5
            print(f"Retrying in {wait_time} seconds...")
            time.sleep(wait_time)
    
    print(f"Failed to fetch {direction} flights after {max_retries} attempts")
    return []

def process_flight_data(flights, direction):
    processed_flights = []
    
    for flight in flights:
        print("DEBUG: Flight item type:", type(flight), flight)
        if not isinstance(flight, dict):
            print("WARNING: Skipping non-dict flight entry:", flight)
            continue
        
        is_arrival = direction == 'inbound'
        flight_time = flight.get('timestamp')
        formatted_time = None
        
        if flight_time:
            try:
                if isinstance(flight_time, str):
                    dt = datetime.fromisoformat(flight_time.replace('Z', '+00:00'))
                    formatted_time = dt.strftime('%Y-%m-%d %H:%M:%S')
                elif isinstance(flight_time, int):
                    dt = datetime.fromtimestamp(flight_time)
                    formatted_time = dt.strftime('%Y-%m-%d %H:%M:%S')
            except (ValueError, TypeError):
                formatted_time = str(flight_time)
        
        flight_record = {
            'flight_id': flight.get('fr24_id', 'N/A'),
            'flight_number': flight.get('flight', flight.get('callsign', 'N/A')),
            'direction': 'Arrival' if is_arrival else 'Departure',
            'actual_time': formatted_time,
            'origin': flight.get('orig_iata', 'N/A'),
            'destination': flight.get('dest_iata', 'N/A'),
            'aircraft_type': flight.get('type', 'N/A'),
            'airline': flight.get('airline', 'N/A'),
            'registration': flight.get('reg', 'N/A')
        }
        
        processed_flights.append(flight_record)
    
    return processed_flights

def analyze_day(date):
    """Analyze flights for a specific day"""
    date_str = date.strftime('%Y-%m-%d')
    print(f"\nAnalyzing flights for {date_str}")
    
    # Fetch arrivals
    arrivals = fetch_flights_for_date(date, 'inbound')
    time.sleep(5)  # Longer delay to avoid rate limiting
    
    # Fetch departures
    departures = fetch_flights_for_date(date, 'outbound')
    
    # Process the data
    processed_arrivals = process_flight_data(arrivals, 'inbound')
    processed_departures = process_flight_data(departures, 'outbound')
    
    # Combine all flights
    all_flights = processed_arrivals + processed_departures
    
    # Sort by actual time
    all_flights = sorted(all_flights, key=lambda x: x['actual_time'] if x['actual_time'] else '')
    
    # Create a daily summary
    daily_summary = {
        'date': date_str,
        'total_flights': len(all_flights),
        'total_arrivals': len(processed_arrivals),
        'total_departures': len(processed_departures),
        'flights': all_flights
    }
    
    # Save the detailed data to a JSON file
    json_path = os.path.join(OUTPUT_DIR, f'bristol_flights_{date_str}.json')
    with open(json_path, 'w') as f:
        json.dump(daily_summary, f, indent=2)
    
    # Save a CSV with the flight list
    csv_path = os.path.join(OUTPUT_DIR, f'bristol_flights_{date_str}.csv')
    with open(csv_path, 'w', newline='') as f:
        # Include all fields that might be in the flight records
        fieldnames = [
            'flight_id', 'flight_number', 'direction', 'actual_time', 
            'origin', 'destination', 'aircraft_type', 'airline', 'registration'
        ]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for flight in all_flights:
            writer.writerow(flight)
    
    print(f"Day summary: {len(all_flights)} total flights ({len(processed_arrivals)} arrivals, {len(processed_departures)} departures)")
    print(f"Data saved to {json_path} and {csv_path}")
    
    return daily_summary

def create_daily_summary_report(all_daily_summaries):
    """Create a summary report of all analyzed days"""
    report_path = os.path.join(OUTPUT_DIR, 'bristol_daily_summary.csv')
    
    with open(report_path, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['Date', 'Total Flights', 'Arrivals', 'Departures'])
        
        for summary in all_daily_summaries:
            writer.writerow([
                summary['date'],
                summary['total_flights'],
                summary['total_arrivals'],
                summary['total_departures']
            ])
    
    print(f"Daily summary report saved to {report_path}")

def main(skip_confirmation=False):
    print("Bristol Airport Flight Analyzer")
    print("===============================")
    
    # Add configuration information
    print(f"API Token: {'*'*4 + API_TOKEN[-4:] if len(API_TOKEN) > 8 else '(Not set - please update)'}")
    print(f"Airport: {BRISTOL_AIRPORT_CODE}")
    print(f"Days to analyze: {DAYS_TO_ANALYZE}")
    print(f"Output directory: {OUTPUT_DIR}")
    print("\nNote: This script uses the historic/flight-positions/full endpoint")
    print("      which costs 8 credits per returned flight.")
    print("      You might need to adjust the 'limit' parameter based on your credit balance.")
    
    # Optional confirmation before proceeding
    if not skip_confirmation:
        try:
            proceed = input("\nProceed with analysis? (y/n): ").lower().strip()
            if proceed != 'y':
                print("Analysis cancelled.")
                return
        except (EOFError, KeyboardInterrupt):
            # If we can't get input, proceed anyway
            print("\nNo input capability detected. Proceeding with analysis...")
    
    daily_summaries = []
    
    # Calculate date range
    end_date = datetime.now()
    start_date = end_date - timedelta(days=DAYS_TO_ANALYZE)
    
    print(f"\nAnalyzing flights from {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
    
    # Loop through each day
    current_date = start_date
    while current_date <= end_date:
        try:
            daily_summary = analyze_day(current_date)
            daily_summaries.append(daily_summary)
            
            # Move to next day
            current_date += timedelta(days=1)
            
            # Add longer delay to avoid rate limiting
            wait_time = 10 + (5 * random.random())  # Between 10-15 seconds
            print(f"Waiting {wait_time:.1f} seconds before processing next day...")
            time.sleep(wait_time)
        except Exception as e:
            print(f"Error analyzing day {current_date.strftime('%Y-%m-%d')}: {e}")
            # Continue with next day after a longer delay
            current_date += timedelta(days=1)
            print("Waiting 30 seconds before continuing due to error...")
            time.sleep(30)
    
    # Create summary report
    create_daily_summary_report(daily_summaries)
    
    print("\nAnalysis complete!")
    print(f"Check the {OUTPUT_DIR} directory for detailed flight data and summary reports.")

if __name__ == "__main__":
    # Check for command line arguments
    if len(sys.argv) > 1:
        # Check for single day mode
        if sys.argv[1].lower() == 'day' and len(sys.argv) > 2:
            try:
                single_date = sys.argv[2]
                # Validate date format
                datetime.strptime(single_date, '%Y-%m-%d')
                print(f"Single day mode activated for {single_date}")
                SINGLE_DAY_MODE = True
                SINGLE_DAY_DATE = single_date
            except ValueError:
                print(f"Invalid date format: {sys.argv[2]}. Please use YYYY-MM-DD format.")
                sys.exit(1)
        # Check for days count
        elif sys.argv[1].lower() == 'days' and len(sys.argv) > 2:
            try:
                days = int(sys.argv[2])
                if days > 0:
                    DAYS_TO_ANALYZE = days
                    print(f"Set analysis period to {days} days")
                else:
                    print("Days must be a positive number")
                    sys.exit(1)
            except ValueError:
                print(f"Invalid number of days: {sys.argv[2]}. Please use a positive integer.")
                sys.exit(1)
        # Check for auto mode (skip confirmation)
        elif sys.argv[1].lower() == 'auto':
            SKIP_CONFIRMATION = True
            print("Auto mode activated - will proceed without confirmation")
    
    # Single day mode processing
    if SINGLE_DAY_MODE and SINGLE_DAY_DATE:
        try:
            date_obj = datetime.strptime(SINGLE_DAY_DATE, '%Y-%m-%d')
            print(f"Analyzing single day: {SINGLE_DAY_DATE}")
            
            # Create output directory
            if not os.path.exists(OUTPUT_DIR):
                os.makedirs(OUTPUT_DIR)
                
            # Analyze the single day
            analyze_day(date_obj)
            print("Single day analysis complete!")
        except Exception as e:
            print(f"Error during single day analysis: {e}")
    else:
        # Run normal multi-day analysis
        main(skip_confirmation=SKIP_CONFIRMATION)