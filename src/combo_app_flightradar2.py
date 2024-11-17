import requests
import json
from datetime import datetime, timedelta, timezone
import logging
import csv
import time
from config import FR24_API_KEY

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s: %(message)s')

class FlightRadar24API:
    def __init__(self, api_key):
        self.base_url = "https://fr24api.flightradar24.com"
        self.headers = {
            'Accept': 'application/json',
            'Accept-Version': 'v1',
            'Authorization': f'Bearer {api_key}'
        }
    
    def categorize_flight(self, flight_time):
        """Categorize flights based on UK night flight restrictions"""
        hour = flight_time.hour
        minute = flight_time.minute
        
        if (23 <= hour) or (hour < 6):
            return "Night flight"
        elif (hour == 6 and minute < 30) or (hour == 22 and minute >= 30):
            return "Shoulder period"
        else:
            return "Day flight"

    def get_historical_flights(self, timestamp):
        """Get historical flight data for Bristol area"""
        # Bristol area bounds
        bounds = "52.0,50.5,-4.0,-1.5"
        
        endpoint = f"{self.base_url}/api/historic/flight-positions/full"
        params = {
            'bounds': bounds,
            'timestamp': timestamp
        }
        
        try:
            response = requests.get(endpoint, headers=self.headers, params=params)
            response.raise_for_status()
            return response.json()
            
        except requests.exceptions.RequestException as e:
            logging.error(f"Error fetching historical flight data: {e}")
            return {'data': []}

    def is_bristol_flight(self, flight):
        """Check if a flight is related to Bristol Airport"""
        if not flight:
            return False
            
        # Check origin and destination
        orig_iata = flight.get('orig_iata')
        orig_icao = flight.get('orig_icao')
        dest_iata = flight.get('dest_iata')
        dest_icao = flight.get('dest_icao')
        
        bristol_codes = ['BRS', 'EGGD']
        return any(code in [orig_iata, orig_icao, dest_iata, dest_icao] for code in bristol_codes)

    def process_flights(self, flights_data, date):
        """Process and categorize flight data"""
        processed_flights = []
        
        # Get the flights from the data response
        flights = flights_data.get('data', [])
        
        for flight in flights:
            try:
                # Check if it's a Bristol flight
                if not self.is_bristol_flight(flight):
                    continue
                
                # Get timestamp
                timestamp_str = flight.get('timestamp')
                if not timestamp_str:
                    continue
                    
                # Convert ISO timestamp to datetime
                flight_time = datetime.strptime(timestamp_str, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
                
                flight_info = {
                    'flight_number': flight.get('flight', 'N/A'),
                    'callsign': flight.get('callsign', 'N/A'),
                    'registration': flight.get('reg', 'N/A'),
                    'aircraft_type': flight.get('type', 'N/A'),
                    'origin_airport': f"{flight.get('orig_icao', 'N/A')}/{flight.get('orig_iata', 'N/A')}",
                    'destination_airport': f"{flight.get('dest_icao', 'N/A')}/{flight.get('dest_iata', 'N/A')}",
                    'altitude': flight.get('alt', 'N/A'),
                    'ground_speed': flight.get('gspeed', 'N/A'),
                    'timestamp': flight_time.strftime("%Y-%m-%d %H:%M:%S %Z"),
                    'category': self.categorize_flight(flight_time),
                    'airline': flight.get('operating_as', 'N/A')
                }
                
                processed_flights.append(flight_info)
                logging.info(f"Found Bristol flight: {flight_info['flight_number']} "
                           f"({flight_info['origin_airport']} → {flight_info['destination_airport']})")
                
            except Exception as e:
                logging.error(f"Error processing flight: {e}")
                continue
        
        # Save to CSV
        if processed_flights:
            filename = f"bristol_flights_{date.strftime('%Y-%m-%d_%H%M')}.csv"
            with open(filename, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=processed_flights[0].keys())
                writer.writeheader()
                writer.writerows(processed_flights)
            
            # Generate summary
            categories = {}
            for flight in processed_flights:
                cat = flight['category']
                categories[cat] = categories.get(cat, 0) + 1
                
            logging.info(f"\nFlight Summary for {date.strftime('%Y-%m-%d %H:%M')}:")
            for category, count in categories.items():
                logging.info(f"{category}: {count} flights")
        else:
            logging.info("No Bristol flights found in this time period")
            
        logging.info(f"Total Bristol flights found: {len(processed_flights)}")
        return processed_flights

def main():
    api = FlightRadar24API(FR24_API_KEY)
    
    # Calculate timestamp for previous day
    yesterday = datetime.now(timezone.utc) - timedelta(days=2)
    start_time = yesterday.replace(hour=0, minute=0, second=0, microsecond=0)
    
    # Process data in 15-minute intervals
    interval = timedelta(minutes=15)
    end_time = start_time + timedelta(days=1)
    
    all_flights = []
    
    current_time = start_time
    while current_time < end_time:
        timestamp = int(current_time.timestamp())
        
        logging.info(f"\nFetching flights for {current_time.strftime('%Y-%m-%d %H:%M:%S %Z')}")
        
        flights_data = api.get_historical_flights(timestamp)
        flights = api.process_flights(flights_data, current_time)
        all_flights.extend(flights)
        
        current_time += interval
        time.sleep(2)  # Rate limiting between requests
    
    # Save daily summary
    if all_flights:
        filename = f"bristol_flights_daily_{start_time.strftime('%Y-%m-%d')}.csv"
        with open(filename, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=all_flights[0].keys())
            writer.writeheader()
            writer.writerows(all_flights)
        
        # Generate daily summary
        categories = {}
        for flight in all_flights:
            cat = flight['category']
            categories[cat] = categories.get(cat, 0) + 1
            
        logging.info(f"\nDaily Flight Summary for {start_time.strftime('%Y-%m-%d')}:")
        for category, count in categories.items():
            logging.info(f"{category}: {count} flights")
        logging.info(f"Total flights: {len(all_flights)}")

if __name__ == "__main__":
    main()