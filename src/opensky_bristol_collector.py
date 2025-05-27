import requests
import json
import time
from datetime import datetime, timedelta
import os
from typing import Dict, List, Optional, Any
import math

class OpenSkyBristolCollector:
    """
    OpenSky Network API collector for Bristol Airport (EGGD)
    Note: OpenSky API provides aircraft positions, not structured flight schedules
    """
    
    def __init__(self, username: Optional[str] = None, password: Optional[str] = None, data_dir: str = "opensky_data"):
        """
        Initialize OpenSky collector
        
        Args:
            username: OpenSky username (optional, increases rate limits)
            password: OpenSky password
            data_dir: Directory to save data
        """
        self.base_url = "https://opensky-network.org/api"
        self.username = username
        self.password = password
        self.data_dir = data_dir
        self.session = requests.Session()
        
        # Bristol Airport coordinates and details
        self.bristol_airport = {
            'icao': 'EGGD',
            'name': 'Bristol Airport',
            'lat': 51.3827,
            'lon': -2.7191,
            'radius_km': 10  # 10km radius around airport
        }
        
        # Set up authentication if provided
        if username and password:
            self.session.auth = (username, password)
            
        os.makedirs(self.data_dir, exist_ok=True)
        
    def haversine_distance(self, lat1: float, lon1: float, lat2: float, lon2: float) -> float:
        """Calculate distance between two points in kilometers"""
        R = 6371  # Earth's radius in kilometers
        
        lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])
        dlat = lat2 - lat1
        dlon = lon2 - lon1
        
        a = math.sin(dlat/2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon/2)**2
        c = 2 * math.asin(math.sqrt(a))
        
        return R * c
    
    def get_states_in_area(self, bbox: Optional[List[float]] = None) -> Dict[str, Any]:
        """
        Get current aircraft states in a bounding box around Bristol Airport
        
        Args:
            bbox: [min_lon, min_lat, max_lon, max_lat] - if None, uses Bristol area
            
        Returns:
            Dictionary with aircraft states
        """
        if bbox is None:
            # Create bounding box around Bristol Airport (approximately 20km x 20km)
            lat_offset = 0.09  # Approximately 10km
            lon_offset = 0.13  # Approximately 10km at Bristol's latitude
            
            bbox = [
                self.bristol_airport['lon'] - lon_offset,  # min_lon
                self.bristol_airport['lat'] - lat_offset,  # min_lat
                self.bristol_airport['lon'] + lon_offset,  # max_lon
                self.bristol_airport['lat'] + lat_offset   # max_lat
            ]
        
        url = f"{self.base_url}/states/all"
        params = {
            'lamin': bbox[1],
            'lomin': bbox[0], 
            'lamax': bbox[3],
            'lomax': bbox[2]
        }
        
        try:
            response = self.session.get(url, params=params, timeout=30)
            response.raise_for_status()
            return response.json()
            
        except requests.exceptions.RequestException as e:
            print(f"Error fetching current states: {e}")
            return {}
    
    def get_arrivals_by_time(self, begin_time: int, end_time: int) -> Dict[str, Any]:
        """
        Get arrivals at Bristol Airport for a time period
        
        Args:
            begin_time: Unix timestamp for start time
            end_time: Unix timestamp for end time
            
        Returns:
            Dictionary with arrival data
        """
        url = f"{self.base_url}/flights/arrival"
        params = {
            'airport': self.bristol_airport['icao'],
            'begin': begin_time,
            'end': end_time
        }
        
        try:
            response = self.session.get(url, params=params, timeout=30)
            response.raise_for_status()
            return response.json()
            
        except requests.exceptions.RequestException as e:
            print(f"Error fetching arrivals: {e}")
            return {}
    
    def get_departures_by_time(self, begin_time: int, end_time: int) -> Dict[str, Any]:
        """
        Get departures from Bristol Airport for a time period
        
        Args:
            begin_time: Unix timestamp for start time
            end_time: Unix timestamp for end time
            
        Returns:
            Dictionary with departure data
        """
        url = f"{self.base_url}/flights/departure"
        params = {
            'airport': self.bristol_airport['icao'],
            'begin': begin_time,
            'end': end_time
        }
        
        try:
            response = self.session.get(url, params=params, timeout=30)
            response.raise_for_status()
            return response.json()
            
        except requests.exceptions.RequestException as e:
            print(f"Error fetching departures: {e}")
            return {}
    
    def get_historical_states(self, time_unix: int, icao24: str = None) -> Dict[str, Any]:
        """
        Get historical aircraft states at a specific time
        
        Args:
            time_unix: Unix timestamp
            icao24: Specific aircraft ICAO24 code (optional)
            
        Returns:
            Dictionary with historical states
        """
        url = f"{self.base_url}/states/all"
        params = {'time': time_unix}
        
        if icao24:
            params['icao24'] = icao24
            
        try:
            response = self.session.get(url, params=params, timeout=30)
            response.raise_for_status()
            return response.json()
            
        except requests.exceptions.RequestException as e:
            print(f"Error fetching historical states: {e}")
            return {}
    
    def process_flight_data(self, flight_data: List[Any], flight_type: str) -> List[Dict[str, Any]]:
        """
        Process OpenSky flight data into a more usable format
        
        Args:
            flight_data: Raw flight data from OpenSky API
            flight_type: 'arrival' or 'departure'
            
        Returns:
            List of processed flight dictionaries
        """
        processed_flights = []
        
        for flight in flight_data:
            try:
                processed_flight = {
                    'icao24': flight.get('icao24', ''),
                    'callsign': flight.get('callsign', '').strip(),
                    'origin_country': flight.get('origin_country', ''),
                    'time_position': flight.get('time_position'),
                    'last_contact': flight.get('last_contact'),
                    'longitude': flight.get('longitude'),
                    'latitude': flight.get('latitude'),
                    'baro_altitude': flight.get('baro_altitude'),
                    'on_ground': flight.get('on_ground'),
                    'velocity': flight.get('velocity'),
                    'true_track': flight.get('true_track'),
                    'vertical_rate': flight.get('vertical_rate'),
                    'geo_altitude': flight.get('geo_altitude'),
                    'squawk': flight.get('squawk'),
                    'spi': flight.get('spi'),
                    'position_source': flight.get('position_source'),
                    'flight_type': flight_type,
                    'timestamp': datetime.utcnow().isoformat(),
                }
                
                # Calculate distance from Bristol Airport if position is available
                if processed_flight['latitude'] and processed_flight['longitude']:
                    distance = self.haversine_distance(
                        self.bristol_airport['lat'], 
                        self.bristol_airport['lon'],
                        processed_flight['latitude'], 
                        processed_flight['longitude']
                    )
                    processed_flight['distance_from_bristol_km'] = distance
                
                processed_flights.append(processed_flight)
                
            except Exception as e:
                print(f"Error processing flight data: {e}")
                continue
        
        return processed_flights
    
    def collect_current_bristol_traffic(self) -> Dict[str, Any]:
        """
        Collect current aircraft traffic around Bristol Airport
        
        Returns:
            Processed traffic data
        """
        print("Collecting current traffic around Bristol Airport...")
        
        # Get current states in Bristol area
        states_data = self.get_states_in_area()
        
        if not states_data or 'states' not in states_data:
            print("No current traffic data available")
            return {}
        
        # Process the states data
        processed_aircraft = []
        bristol_lat = self.bristol_airport['lat']
        bristol_lon = self.bristol_airport['lon']
        
        for state in states_data['states']:
            try:
                aircraft = {
                    'icao24': state[0],
                    'callsign': state[1].strip() if state[1] else '',
                    'origin_country': state[2],
                    'time_position': state[3],
                    'last_contact': state[4],
                    'longitude': state[5],
                    'latitude': state[6],
                    'baro_altitude': state[7],
                    'on_ground': state[8],
                    'velocity': state[9],
                    'true_track': state[10],
                    'vertical_rate': state[11],
                    'geo_altitude': state[13],
                    'squawk': state[14],
                    'spi': state[15],
                    'position_source': state[16],
                    'timestamp': datetime.utcnow().isoformat()
                }
                
                # Calculate distance from Bristol Airport
                if aircraft['latitude'] and aircraft['longitude']:
                    distance = self.haversine_distance(
                        bristol_lat, bristol_lon,
                        aircraft['latitude'], aircraft['longitude']
                    )
                    aircraft['distance_from_bristol_km'] = distance
                    
                    # Only include aircraft within 15km of Bristol Airport
                    if distance <= 15:
                        processed_aircraft.append(aircraft)
                
            except Exception as e:
                print(f"Error processing aircraft state: {e}")
                continue
        
        result = {
            'timestamp': datetime.utcnow().isoformat(),
            'airport': self.bristol_airport,
            'aircraft_count': len(processed_aircraft),
            'aircraft': processed_aircraft,
            'data_source': 'OpenSky Network'
        }
        
        return result
    
    def collect_historical_flights(self, date: datetime) -> Dict[str, Any]:
        """
        Collect historical flight data for Bristol Airport for a specific date
        
        Args:
            date: Date to collect data for
            
        Returns:
            Combined arrivals and departures data
        """
        # Convert date to Unix timestamps (start and end of day)
        start_of_day = date.replace(hour=0, minute=0, second=0, microsecond=0)
        end_of_day = date.replace(hour=23, minute=59, second=59, microsecond=999999)
        
        begin_time = int(start_of_day.timestamp())
        end_time = int(end_of_day.timestamp())
        
        print(f"Collecting historical flights for {date.strftime('%Y-%m-%d')}...")
        
        # Get arrivals and departures
        arrivals_data = self.get_arrivals_by_time(begin_time, end_time)
        departures_data = self.get_departures_by_time(begin_time, end_time)
        
        # Process the data
        arrivals = []
        departures = []
        
        if arrivals_data and isinstance(arrivals_data, list):
            arrivals = self.process_flight_data(arrivals_data, 'arrival')
            
        if departures_data and isinstance(departures_data, list):
            departures = self.process_flight_data(departures_data, 'departure')
        
        result = {
            'date': date.strftime('%Y-%m-%d'),
            'airport': self.bristol_airport,
            'arrivals': arrivals,
            'departures': departures,
            'summary': {
                'total_flights': len(arrivals) + len(departures),
                'arrivals_count': len(arrivals),
                'departures_count': len(departures),
                'data_source': 'OpenSky Network'
            },
            'timestamp': datetime.utcnow().isoformat()
        }
        
        return result
    
    def save_data(self, data: Dict[str, Any], filename: str):
        """Save data to JSON file"""
        filepath = os.path.join(self.data_dir, filename)
        with open(filepath, 'w') as f:
            json.dump(data, f, indent=2)
        print(f"Data saved to {filepath}")
    
    def collect_daily_comparison(self, date: datetime):
        """
        Collect a day's worth of data for comparison with AviationStack
        
        Args:
            date: Date to collect data for
        """
        print(f"=== Collecting OpenSky data for {date.strftime('%Y-%m-%d')} ===")
        
        # Collect historical flights
        flight_data = self.collect_historical_flights(date)
        
        # Save the data
        filename = f"opensky_bristol_{date.strftime('%Y-%m-%d')}.json"
        self.save_data(flight_data, filename)
        
        # Print summary
        print(f"Summary for {date.strftime('%Y-%m-%d')}:")
        print(f"  Total flights: {flight_data['summary']['total_flights']}")
        print(f"  Arrivals: {flight_data['summary']['arrivals_count']}")
        print(f"  Departures: {flight_data['summary']['departures_count']}")
        
        return flight_data
    
    def collect_current_snapshot(self):
        """Collect current aircraft traffic snapshot"""
        print("=== Collecting current traffic snapshot ===")
        
        current_data = self.collect_current_bristol_traffic()
        
        if current_data:
            # Save current snapshot
            filename = f"opensky_bristol_current_{datetime.now().strftime('%Y-%m-%d_%H-%M')}.json"
            self.save_data(current_data, filename)
            
            print(f"Current traffic summary:")
            print(f"  Aircraft in Bristol area: {current_data['aircraft_count']}")
            
            # Show some details about nearby aircraft
            for aircraft in current_data['aircraft'][:5]:  # Show first 5
                callsign = aircraft['callsign'] or 'Unknown'
                distance = aircraft.get('distance_from_bristol_km', 0)
                altitude = aircraft.get('baro_altitude', 'Unknown')
                on_ground = aircraft.get('on_ground', False)
                
                print(f"  - {callsign}: {distance:.1f}km away, {altitude}m altitude, {'on ground' if on_ground else 'airborne'}")
        
        return current_data

def compare_with_aviationstack(opensky_data: Dict[str, Any], aviationstack_file: str):
    """
    Compare OpenSky data with AviationStack data
    
    Args:
        opensky_data: OpenSky flight data
        aviationstack_file: Path to AviationStack JSON file
    """
    try:
        with open(aviationstack_file, 'r') as f:
            aviationstack_data = json.load(f)
        
        print("\n=== Data Comparison ===")
        print(f"OpenSky - Total flights: {opensky_data['summary']['total_flights']}")
        print(f"AviationStack - Total flights: {aviationstack_data['summary']['total_flights']}")
        
        print(f"\nOpenSky - Arrivals: {opensky_data['summary']['arrivals_count']}")
        print(f"AviationStack - Arrivals: {aviationstack_data['summary']['arrivals']}")
        
        print(f"\nOpenSky - Departures: {opensky_data['summary']['departures_count']}")
        print(f"AviationStack - Departures: {aviationstack_data['summary']['departures']}")
        
        print("\nNote: OpenSky and AviationStack use different data sources and methods,")
        print("so some differences in flight counts are expected.")
        
    except FileNotFoundError:
        print(f"AviationStack file {aviationstack_file} not found for comparison")
    except Exception as e:
        print(f"Error comparing data: {e}")

# Example usage
if __name__ == "__main__":
    # Initialize collector
    # You can optionally provide OpenSky credentials to increase rate limits
    collector = OpenSkyBristolCollector(
        # username="your_opensky_username",  # Optional
        # password="your_opensky_password"   # Optional
    )
    
    # Collect current traffic snapshot
    current_data = collector.collect_current_snapshot()
    
    # Collect historical data for yesterday
    yesterday = datetime.now() - timedelta(days=1)
    historical_data = collector.collect_daily_comparison(yesterday)
    
    # Optional: Compare with AviationStack data if you have it
    # compare_with_aviationstack(historical_data, "EGGD_2025-05-23.json")
    
    print("\nNote: OpenSky Network provides different data than AviationStack:")
    print("- OpenSky: Real-time aircraft positions and some flight tracking")
    print("- AviationStack: Structured flight schedules and commercial flight data")
    print("- OpenSky may show fewer commercial flights but more general aviation")
    print("- Rate limits: 4000 requests/day (anonymous), 400 requests/day (registered)")