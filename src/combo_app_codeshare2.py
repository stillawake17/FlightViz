import requests
import json
import os
from datetime import datetime, timedelta
from config import api_key  # Assuming your API key is stored in a separate config file

class FlightDataCollector:
    def __init__(self, airport_code, api_key):
        self.base_url = "http://api.aviationstack.com/v1/flights"
        self.airport_code = airport_code
        self.api_key = api_key
        self.data_dir = "data"
        
        # Ensure data directory exists
        if not os.path.exists(self.data_dir):
            os.makedirs(self.data_dir)
    
    def fetch_flights(self, date, is_arrival=True, limit=100, max_offset=10000):
        """
        Fetch flight data from Aviation Stack API with pagination.
        Stops when the API indicates there are no more flights or the max_offset is reached.
        """
        all_flights = []
        params = {
            'access_key': self.api_key,
            'flight_date': date,
            'limit': limit,
            'offset': 0
        }
        
        # Set airport code for either arrivals or departures
        if is_arrival:
            params['arr_icao'] = self.airport_code
        else:
            params['dep_icao'] = self.airport_code
        
        print(f"Fetching {'arrivals' if is_arrival else 'departures'} for {date}")
        
        # Paginate through all results
        while params['offset'] < max_offset:
            try:
                response = requests.get(self.base_url, params=params)
                response.raise_for_status()  # Raise exception for HTTP errors
                
                data = response.json()
                flights = data.get('data', [])
                
                # If no flights are returned, break out of the loop.
                if not flights:
                    break
                    
                all_flights.extend(flights)
                print(f"Retrieved {len(flights)} flights (offset: {params['offset']})")
                
                # Use pagination info if available to determine when to stop
                pagination = data.get('pagination', {})
                total = pagination.get('total')
                if total is not None:
                    if params['offset'] + len(flights) >= total:
                        break
                
                # Check if we've retrieved fewer flights than the limit (indicating end of data)
                if len(flights) < params['limit']:
                    break
                
                # Increment offset for next page
                params['offset'] += params['limit']
                
            except requests.exceptions.RequestException as e:
                print(f"Error fetching data: {e}")
                if hasattr(e, 'response') and e.response is not None:
                    print(f"Response: {e.response.text}")
                break
        
        return all_flights
    
    def categorize_flight(self, time_str):
        """
        Categorize flight based on its actual time according to:
        
        Shoulder Flights: 11:00–11:29:59 PM & 6:00–6:59:59 AM 
        Night Flights: 11:30 PM–5:59:59 AM
        """
        if not time_str:
            return "Unknown"
            
        try:
            dt = datetime.fromisoformat(time_str.replace('Z', '+00:00'))
            hour = dt.hour
            minute = dt.minute
            
            if hour == 23:
                if minute < 30:
                    return "Shoulder hour flights"
                else:
                    return "Night hour flights"
            elif hour < 6:
                return "Night hour flights"
            elif hour == 6:
                return "Shoulder hour flights"
            else:
                return "Regular flights"
        except (ValueError, TypeError) as e:
            print(f"Error parsing time '{time_str}': {e}")
            return "Unknown"
    
    def process_codeshare_flights(self, flights):
        """
        Process flights to handle codeshares properly.
        Returns a list where duplicate codeshare flights are removed.
        """
        processed_flights = []
        seen_primary_numbers = set()
    
        for flight in flights:
            flight_info = flight.get('flight', {})
            codeshared = flight_info.get('codeshared')
            flight_number = flight_info.get('number')
        
            # Determine the primary flight number: if codeshared exists, use its flight_number; else use flight's own number
            primary_number = codeshared.get('flight_number') if codeshared else flight_number
        
            # Skip if we've already processed this primary flight
            if primary_number in seen_primary_numbers:
                continue
        
            seen_primary_numbers.add(primary_number)
        
            # Determine if this flight is an arrival or departure based on the _query (if available)
            is_arrival = 'arr_icao' in flight.get('_query', {})
            time_field = flight.get('arrival' if is_arrival else 'departure', {}).get('actual')
            flight['time_category'] = self.categorize_flight(time_field)
        
            processed_flights.append(flight)
        
        return processed_flights

    
    def collect_daily_data(self, date_str=None):
        """
        Collect flight data for a specific date or yesterday by default.
        """
        # Use yesterday's date if not specified
        if not date_str:
            date_str = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
            
        # Fetch arrival and departure data
        arrivals = self.fetch_flights(date_str, is_arrival=True)
        departures = self.fetch_flights(date_str, is_arrival=False)
        
        # Process to handle codeshares
        processed_arrivals = self.process_codeshare_flights(arrivals)
        processed_departures = self.process_codeshare_flights(departures)
        
        print(f"Processed arrivals count: {len(processed_arrivals)}")
        print(f"Processed departures count: {len(processed_departures)}")
        
        # Combine data
        combined_data = {
            'date': date_str,
            'arrivals': processed_arrivals,
            'departures': processed_departures
        }
        
        # Calculate summary statistics
        summary = self.generate_summary(combined_data)
        combined_data['summary'] = summary
        
        # Save file
        filename = f"{self.data_dir}/{self.airport_code}_combined_{date_str}.json"
        with open(filename, 'w') as file:
            json.dump(combined_data, file, indent=4)
            
        print(f"Combined data saved to {filename}")
        return combined_data
    
    def generate_summary(self, data):
        """
        Generate summary statistics for the flight data.
        """
        # Categories to count
        categories = {
            'Regular flights': 0,
            'Shoulder hour flights': 0,
            'Night hour flights': 0,
            'Unknown': 0
        }
        
        # Count arrivals by category
        arrival_categories = {}
        for flight in data['arrivals']:
            category = flight.get('time_category', 'Unknown')
            categories[category] = categories.get(category, 0) + 1
            arrival_categories[category] = arrival_categories.get(category, 0) + 1
        
        # Count departures by category
        departure_categories = {}
        for flight in data['departures']:
            category = flight.get('time_category', 'Unknown')
            categories[category] = categories.get(category, 0) + 1
            departure_categories[category] = departure_categories.get(category, 0) + 1
        
        # Count flights by airline for night flights
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
        
        # Create summary
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
    # collector.collect_daily_data('2025-03-16')
