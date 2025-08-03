import requests
import json
import os
from datetime import datetime, timedelta
from config import api_key

class FlightDataCollector:
    def __init__(self, airport_code, api_key):
        self.base_url = "http://api.aviationstack.com/v1/flights"
        self.airport_code = airport_code
        self.api_key = api_key
        self.data_dir = "data"
        
        if not os.path.exists(self.data_dir):
            os.makedirs(self.data_dir)
    
    def fetch_flights(self, date, is_arrival=True, limit=100, max_offset=10000):
        """Fetch flight data with improved error handling and debugging"""
        all_flights = []
        params = {
            'access_key': self.api_key,
            'flight_date': date,
            'limit': limit,
            'offset': 0
        }
        
        # Try both ICAO and IATA codes for Bristol
        if is_arrival:
            params['arr_icao'] = self.airport_code  # EGGD
            # Also try IATA code as fallback
            # params['arr_iata'] = 'BRS'  # Uncomment if ICAO doesn't work
        else:
            params['dep_icao'] = self.airport_code
            # params['dep_iata'] = 'BRS'  # Uncomment if ICAO doesn't work
        
        print(f"Fetching {'arrivals' if is_arrival else 'departures'} for {date}")
        print(f"Using airport code: {self.airport_code}")
        
        while params['offset'] < max_offset:
            try:
                print(f"Making API request with offset: {params['offset']}")
                response = requests.get(self.base_url, params=params)
                response.raise_for_status()
                
                data = response.json()
                
                # Debug: Print API response structure
                print(f"API Response keys: {list(data.keys())}")
                if 'pagination' in data:
                    print(f"Pagination info: {data['pagination']}")
                
                flights = data.get('data', [])
                
                if not flights:
                    print("No more flights returned, stopping pagination")
                    break
                    
                all_flights.extend(flights)
                print(f"Retrieved {len(flights)} flights (total so far: {len(all_flights)})")
                
                # Check pagination
                pagination = data.get('pagination', {})
                total = pagination.get('total', 0)
                print(f"Total flights available: {total}")
                
                if total > 0 and params['offset'] + len(flights) >= total:
                    print("Reached end of available data")
                    break
                
                if len(flights) < params['limit']:
                    print("Received fewer flights than limit, assuming end of data")
                    break
                
                params['offset'] += params['limit']
                
            except requests.exceptions.RequestException as e:
                print(f"ERROR fetching data: {e}")
                if hasattr(e, 'response') and e.response is not None:
                    print(f"Response status: {e.response.status_code}")
                    print(f"Response text: {e.response.text}")
                break
            except json.JSONDecodeError as e:
                print(f"ERROR parsing JSON response: {e}")
                break
        
        print(f"Final count: {len(all_flights)} {'arrivals' if is_arrival else 'departures'}")
        return all_flights
    
    def categorize_flight(self, time_str):
       if not time_str:
        return "Unknown"
        
        try:
        # Handle ISO format with timezone
            if 'T' in time_str:
                # Remove 'Z' and handle timezone properly
                if time_str.endswith('Z'):
                    time_str = time_str[:-1] + '+00:00'
                dt = datetime.fromisoformat(time_str)
            else:
                # For simple time format, use today's date
                today = datetime.now().date()
                time_only = datetime.strptime(time_str, '%H:%M').time()
                dt = datetime.combine(today, time_only)
        
            hour = dt.hour
            
            if hour == 23 and dt.minute >= 30:
                return "Night hour flights"
            elif hour < 6 or hour == 23:
                return "Night hour flights"
            elif hour == 6:
                return "Shoulder hour flights"
            else:
                return "Regular flights"
        except (ValueError, TypeError) as e:
            print(f"Error parsing time '{time_str}': {e}")
        return "Unknown"
    
    def process_codeshare_flights(self, flights, debug_codeshares=True):
        """
        Process flights with less aggressive codeshare handling
        """
        processed_flights = []
        seen_flights = set()
        codeshare_count = 0
        
        for flight in flights:
            flight_info = flight.get('flight', {})
            codeshared = flight_info.get('codeshared')
            flight_number = flight_info.get('number')
            
            # Create a unique identifier for the flight
            # Use actual departure/arrival time + route to identify duplicates
            departure_info = flight.get('departure', {})
            arrival_info = flight.get('arrival', {})
            
            # Create flight signature based on time and route, not just flight number
            flight_signature = (
                departure_info.get('scheduled', ''),
                arrival_info.get('scheduled', ''),
                departure_info.get('airport', ''),
                arrival_info.get('airport', '')
            )
            
            if codeshared:
                codeshare_count += 1
                if debug_codeshares:
                    print(f"Codeshare found: {flight_number} operates as {codeshared.get('flight_number')}")
            
            # Only skip if we have the exact same flight signature
            if flight_signature in seen_flights:
                if debug_codeshares:
                    print(f"Skipping duplicate flight: {flight_number}")
                continue
            
            seen_flights.add(flight_signature)
            
            # Determine arrival/departure and categorize
            # Determine arrival/departure by matching ICAO code
            is_arrival = flight.get('arrival', {}).get('icao') == self.airport_code

            # now pick the right side once
            # determine arrival vs departure upstream…
            side = 'arrival' if is_arrival else 'departure'
            info = flight.get(side, {})

            # replacement for your original two‐liner
            time_field = info.get('actual') or info.get('estimated') or info.get('scheduled')

            flight['time_category'] = self.categorize_flight(time_field)
            processed_flights.append(flight)
        
        print(f"Processed {len(processed_flights)} unique flights from {len(flights)} raw flights")
        print(f"Found {codeshare_count} codeshare flights")
        return processed_flights
    
    def collect_daily_data(self, date_str=None):
        """
        Collect flight data
        """
        if not date_str:
            # FIX: Use yesterday instead of 32 days ago
            date_str = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        
        print(f"Collecting data for date: {date_str}")
        
        # Fetch data
        arrivals = self.fetch_flights(date_str, is_arrival=True)
        departures = self.fetch_flights(date_str, is_arrival=False)
        
        print(f"Raw arrivals: {len(arrivals)}")
        print(f"Raw departures: {len(departures)}")
        
        # Process codeshares with debugging
        processed_arrivals = self.process_codeshare_flights(arrivals, debug_codeshares=True)
        processed_departures = self.process_codeshare_flights(departures, debug_codeshares=True)
        
        print(f"Final processed arrivals: {len(processed_arrivals)}")
        print(f"Final processed departures: {len(processed_departures)}")
        
        # Rest of the method remains the same...
        combined_data = {
            'date': date_str,
            'arrivals': processed_arrivals,
            'departures': processed_departures
        }
        
        summary = self.generate_summary(combined_data)
        combined_data['summary'] = summary
        
        filename = f"{self.data_dir}/{self.airport_code}_combined_{date_str}.json"
        with open(filename, 'w') as file:
            json.dump(combined_data, file, indent=4)
            
        print(f"Combined data saved to {filename}")
        print(f"Summary: {summary['total_flights']} total flights ({summary['arrivals']} arrivals, {summary['departures']} departures)")
        
        return combined_data
    
    def generate_summary(self, data):
        """Generate summary statistics - unchanged"""
        categories = {
            'Regular flights': 0,
            'Shoulder hour flights': 0,
            'Night hour flights': 0,
            'Unknown': 0
        }
        
        arrival_categories = {}
        for flight in data['arrivals']:
            category = flight.get('time_category', 'Unknown')
            categories[category] = categories.get(category, 0) + 1
            arrival_categories[category] = arrival_categories.get(category, 0) + 1
        
        departure_categories = {}
        for flight in data['departures']:
            category = flight.get('time_category', 'Unknown')
            categories[category] = categories.get(category, 0) + 1
            departure_categories[category] = departure_categories.get(category, 0) + 1
        
        night_airlines = {}
        night_origins = {}
        
        for flight in data['arrivals']:
            if flight.get('time_category') == 'Night hour flights':
                airline = flight.get('airline', {}).get('name', 'Unknown')
                night_airlines[airline] = night_airlines.get(airline, 0) + 1
                
                origin = flight.get('departure', {}).get('airport', 'Unknown')
                night_origins[origin] = night_origins.get(origin, 0) + 1
        
        for flight in data['departures']:
            if flight.get('time_category') == 'Night hour flights':
                airline = flight.get('airline', {}).get('name', 'Unknown')
                night_airlines[airline] = night_airlines.get(airline, 0) + 1
        
        summary = {
            'total_flights': len(data['arrivals']) + len(data['departures']),
            'arrivals': len(data['arrivals']),
            'departures': len(data['departures']),
            'categories': categories,
            'arrival_categories': arrival_categories,
            'departure_categories': departure_categories,
            'night_flights': {
                'total': categories.get('Night hour flights', 0),
                'airlines': night_airlines,
                'origins': night_origins
            }
        }
        
        return summary

# Main execution
if __name__ == "__main__":
    # Create collector with your airport code
    collector = FlightDataCollector('EGGD', api_key)
    
    # Collect yesterday's data by default
    collector.collect_daily_data()
    
    # You can also specify a date
    # collector.collect_daily_data('2025-07-24')