import requests
import json
import os
from datetime import datetime, timedelta
from collections import defaultdict
from config import api_key

class FlightDataCollectorDebug:
    def __init__(self, airport_code, api_key):
        self.base_url = "http://api.aviationstack.com/v1/flights"
        self.airport_code = airport_code
        self.api_key = api_key
        self.data_dir = "data"
        
        if not os.path.exists(self.data_dir):
            os.makedirs(self.data_dir)
    
    def debug_flight_times(self, flights, flight_type="arrivals"):
        """Debug function to see what time fields are actually populated"""
        time_field_stats = defaultdict(int)
        sample_flights = []
        
        print(f"\n=== Debugging {flight_type.upper()} ===")
        print(f"Total flights: {len(flights)}")
        
        for i, flight in enumerate(flights):
            flight_info = flight.get('flight', {})
            
            # Check both arrival and departure sections
            arrival_info = flight.get('arrival', {})
            departure_info = flight.get('departure', {})
            
            # Determine which side we care about based on flight type
            time_info = arrival_info if flight_type == "arrivals" else departure_info
            
            # Check all possible time fields
            actual = time_info.get('actual')
            estimated = time_info.get('estimated')
            scheduled = time_info.get('scheduled')
            
            # Track which fields are populated
            if actual:
                time_field_stats['actual'] += 1
            if estimated:
                time_field_stats['estimated'] += 1
            if scheduled:
                time_field_stats['scheduled'] += 1
            
            # Collect sample flights for detailed inspection
            if i < 5:  # First 5 flights
                sample_flights.append({
                    'flight_number': flight_info.get('number', 'Unknown'),
                    'airline': flight.get('airline', {}).get('name', 'Unknown'),
                    'actual': actual,
                    'estimated': estimated,
                    'scheduled': scheduled,
                    'arrival_airport': arrival_info.get('airport', 'Unknown'),
                    'departure_airport': departure_info.get('airport', 'Unknown'),
                    'arrival_icao': arrival_info.get('icao', 'Unknown'),
                    'departure_icao': departure_info.get('icao', 'Unknown'),
                })
        
        # Print statistics
        print(f"\nTime field population statistics:")
        for field, count in time_field_stats.items():
            percentage = (count / len(flights) * 100) if flights else 0
            print(f"  {field}: {count}/{len(flights)} ({percentage:.1f}%)")
        
        # Print sample flights
        print(f"\nSample {flight_type}:")
        for i, sample in enumerate(sample_flights):
            print(f"\n  Flight {i+1}: {sample['flight_number']} ({sample['airline']})")
            print(f"    Route: {sample['departure_airport']} -> {sample['arrival_airport']}")
            print(f"    ICAO: {sample['departure_icao']} -> {sample['arrival_icao']}")
            print(f"    Actual: {sample['actual']}")
            print(f"    Estimated: {sample['estimated']}")
            print(f"    Scheduled: {sample['scheduled']}")
        
        # Save raw data for inspection
        debug_filename = f"{self.data_dir}/debug_{flight_type}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(debug_filename, 'w') as f:
            json.dump(flights[:10], f, indent=2)  # Save first 10 flights
        print(f"\nRaw data sample saved to: {debug_filename}")
        
        return time_field_stats
    
    def test_api_response(self, date_str=None):
        """Test the API with a simple request to see what we get"""
        if not date_str:
            date_str = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        
        print(f"\n=== TESTING API RESPONSE FOR {date_str} ===")
        
        # Try a simple request first
        params = {
            'access_key': self.api_key,
            'flight_date': date_str,
            'limit': 5,  # Just get 5 flights to test
            'arr_icao': self.airport_code
        }
        
        print(f"Request parameters: {json.dumps(params, indent=2)}")
        print(f"Request URL: {self.base_url}")
        
        try:
            response = requests.get(self.base_url, params=params)
            print(f"Response status: {response.status_code}")
            
            if response.status_code != 200:
                print(f"Error response: {response.text}")
                return None
            
            data = response.json()
            
            # Check if we have an error message
            if 'error' in data:
                print(f"API Error: {data['error']}")
                return None
            
            # Print the entire response structure
            print(f"\nAPI Response structure:")
            print(f"Keys in response: {list(data.keys())}")
            
            if 'pagination' in data:
                print(f"\nPagination info: {json.dumps(data['pagination'], indent=2)}")
            
            flights = data.get('data', [])
            print(f"\nNumber of flights returned: {len(flights)}")
            
            if flights:
                print(f"\nFirst flight full structure:")
                print(json.dumps(flights[0], indent=2))
            
            return data
            
        except Exception as e:
            print(f"Exception occurred: {type(e).__name__}: {e}")
            return None
    
    def collect_and_debug(self, date_str=None):
        """Collect data with extensive debugging"""
        if not date_str:
            date_str = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        
        print(f"Collecting and debugging data for: {date_str}")
        print(f"Airport code: {self.airport_code}")
        
        # First, test the API
        test_response = self.test_api_response(date_str)
        
        if not test_response:
            print("\nAPI test failed. Check your API key and parameters.")
            return None
        
        # If test passed, fetch full data
        print("\n" + "="*50)
        print("FETCHING FULL DATASET")
        print("="*50)
        
        # Fetch arrivals
        arrivals = self.fetch_flights_debug(date_str, is_arrival=True)
        
        # Fetch departures
        departures = self.fetch_flights_debug(date_str, is_arrival=False)
        
        # Debug the time fields
        if arrivals:
            self.debug_flight_times(arrivals, "arrivals")
        else:
            print("\nNo arrivals found!")
        
        if departures:
            self.debug_flight_times(departures, "departures")
        else:
            print("\nNo departures found!")
        
        return {
            'arrivals': arrivals,
            'departures': departures,
            'date': date_str
        }
    
    def fetch_flights_debug(self, date, is_arrival=True, limit=100):
        """Fetch flights with debugging info"""
        all_flights = []
        params = {
            'access_key': self.api_key,
            'flight_date': date,
            'limit': limit,
            'offset': 0
        }
        
        if is_arrival:
            params['arr_icao'] = self.airport_code
        else:
            params['dep_icao'] = self.airport_code
        
        flight_type = "arrivals" if is_arrival else "departures"
        print(f"\nFetching {flight_type} for {date}")
        
        try:
            response = requests.get(self.base_url, params=params)
            response.raise_for_status()
            
            data = response.json()
            flights = data.get('data', [])
            
            print(f"Retrieved {len(flights)} {flight_type}")
            
            return flights
            
        except Exception as e:
            print(f"Error fetching {flight_type}: {e}")
            return []

# Alternative API Test
def test_alternative_params(api_key, airport_code='EGGD'):
    """Test different parameter combinations"""
    print("\n=== TESTING ALTERNATIVE PARAMETERS ===")
    
    collector = FlightDataCollectorDebug(airport_code, api_key)
    base_url = "http://api.aviationstack.com/v1/flights"
    
    # Test different parameter combinations
    test_params = [
        {
            'name': 'IATA code instead of ICAO',
            'params': {
                'access_key': api_key,
                'flight_date': (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d"),
                'arr_iata': 'BRS',  # Bristol IATA code
                'limit': 5
            }
        },
        {
            'name': 'Without date (live data)',
            'params': {
                'access_key': api_key,
                'arr_icao': airport_code,
                'limit': 5
            }
        },
        {
            'name': 'Different date format',
            'params': {
                'access_key': api_key,
                'flight_date': datetime.now().strftime("%Y-%m-%d"),  # Today instead of yesterday
                'arr_icao': airport_code,
                'limit': 5
            }
        }
    ]
    
    for test in test_params:
        print(f"\n--- Testing: {test['name']} ---")
        print(f"Parameters: {json.dumps(test['params'], indent=2)}")
        
        try:
            response = requests.get(base_url, params=test['params'])
            print(f"Status: {response.status_code}")
            
            if response.status_code == 200:
                data = response.json()
                flights = data.get('data', [])
                print(f"Flights returned: {len(flights)}")
                
                if flights and len(flights) > 0:
                    # Check first flight for time fields
                    first_flight = flights[0]
                    arrival = first_flight.get('arrival', {})
                    print(f"First flight arrival times:")
                    print(f"  Scheduled: {arrival.get('scheduled')}")
                    print(f"  Estimated: {arrival.get('estimated')}")
                    print(f"  Actual: {arrival.get('actual')}")
            else:
                print(f"Error: {response.text[:200]}...")
                
        except Exception as e:
            print(f"Exception: {e}")

# Main execution
if __name__ == "__main__":
    # Run the debug collector
    debug_collector = FlightDataCollectorDebug('EGGD', api_key)
    
    # First, test the API response
    print("Step 1: Testing API response...")
    debug_collector.test_api_response()
    
    print("\n" + "="*70 + "\n")
    
    # Then collect and debug full data
    print("Step 2: Collecting and debugging full dataset...")
    result = debug_collector.collect_and_debug()
    
    print("\n" + "="*70 + "\n")
    
    # Test alternative parameters
    print("Step 3: Testing alternative parameters...")
    test_alternative_params(api_key)