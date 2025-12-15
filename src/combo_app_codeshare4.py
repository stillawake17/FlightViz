import requests
import datetime
import json
import csv
from config import api_key

# Replace with your actual AviationStack API key
API_KEY = api_key

def get_flights_from_brs(date=None):
    """
    Fetches flights departing from Bristol Airport (BRS) on a given date.
    Uses pagination to get ALL flights, not just the first 100.

    Args:
        date (str, optional): The date to query in YYYY-MM-DD format.
                              Defaults to yesterday.

    Returns:
        list: A list of flight dictionaries, or None if an error occurs.
    """
    base_url = "http://api.aviationstack.com/v1/flights"
    all_flights = []
    offset = 0
    limit = 100
    
    while True:
        # FIXED: Use pagination with offset
        params = {
            "access_key": API_KEY,
            "dep_iata": "BRS",
            "flight_date": date if date else get_yesterday(),
            "limit": limit,
            "offset": offset
        }
        
        try:
            print(f"Fetching departures from BRS (offset: {offset})")
            response = requests.get(base_url, params=params)
            response.raise_for_status()
            data = response.json()
            
            flights = data.get("data", [])
            
            # If no flights returned, we've reached the end
            if not flights:
                print(f"No more departures found at offset {offset}")
                break
            
            all_flights.extend(flights)
            print(f"Retrieved {len(flights)} departures (total so far: {len(all_flights)})")
            
            # Check pagination info to see if there are more results
            pagination = data.get('pagination', {})
            total = pagination.get('total', 0)
            
            if total > 0 and offset + len(flights) >= total:
                print(f"Reached end: {offset + len(flights)} of {total} total departures")
                break
            
            # If we got fewer flights than the limit, we're done
            if len(flights) < limit:
                print(f"Got {len(flights)} flights (less than limit {limit}), stopping")
                break
            
            # Move to next page
            offset += limit
            
        except requests.exceptions.RequestException as e:
            print(f"Error fetching departures from BRS: {e}")
            if hasattr(e, 'response') and e.response is not None:
                print(f"Response status: {e.response.status_code}")
                print(f"Response text: {e.response.text}")
            break
    
    print(f"Total departures found: {len(all_flights)}")
    return all_flights

def get_flights_to_brs(date=None):
    """
    Fetches flights arriving at Bristol Airport (BRS) on a given date.
    Uses pagination to get ALL flights, not just the first 100.

    Args:
        date (str, optional): The date to query in YYYY-MM-DD format.
                              Defaults to yesterday.

    Returns:
        list: A list of flight dictionaries, or None if an error occurs.
    """
    base_url = "http://api.aviationstack.com/v1/flights"
    all_flights = []
    offset = 0
    limit = 100
    
    while True:
        # FIXED: Use pagination with offset for arrivals
        params = {
            "access_key": API_KEY,
            "arr_iata": "BRS",
            "flight_date": date if date else get_yesterday(),
            "limit": limit,
            "offset": offset
        }

        try:
            print(f"Fetching arrivals to BRS (offset: {offset})")
            response = requests.get(base_url, params=params)
            response.raise_for_status()
            data = response.json()
            
            flights = data.get("data", [])
            
            # If no flights returned, we've reached the end
            if not flights:
                print(f"No more arrivals found at offset {offset}")
                break
                
            all_flights.extend(flights)
            print(f"Retrieved {len(flights)} arrivals (total so far: {len(all_flights)})")
            
            # Check pagination info
            pagination = data.get('pagination', {})
            total = pagination.get('total', 0)
            
            if total > 0 and offset + len(flights) >= total:
                print(f"Reached end: {offset + len(flights)} of {total} total arrivals")
                break
            
            # If we got fewer flights than the limit, we're done
            if len(flights) < limit:
                print(f"Got {len(flights)} flights (less than limit {limit}), stopping")
                break
            
            # Move to next page
            offset += limit
            
        except requests.exceptions.RequestException as e:
            print(f"Error fetching arrivals to BRS: {e}")
            if hasattr(e, 'response') and e.response is not None:
                print(f"Response status: {e.response.status_code}")
                print(f"Response text: {e.response.text}")
            break
    
    print(f"Total arrivals found: {len(all_flights)}")
    return all_flights

def get_yesterday():
    """
    Returns yesterday's date in YYYY-MM-DD format.
    """
    today = datetime.date.today()
    yesterday = today - datetime.timedelta(days=90)
    return yesterday.strftime("%Y-%m-%d")

def analyze_flights(flights):
    """
    Analyzes a list of flights to extract departure/arrival times.

    Args:
        flights (list): A list of flight dictionaries.

    Returns:
        dict: A dictionary containing departure and arrival times for each flight.
              The keys are 'departures' and 'arrivals', with lists of times.
    """
    departures = []
    arrivals = []

    if flights:
        for flight in flights:
            # FIXED: Use correct field structure from AviationStack API
            if "departure" in flight and flight["departure"]:
                # Get scheduled time, then actual, then estimated
                dep_time = (flight["departure"].get("actual") or 
                           flight["departure"].get("scheduled") or 
                           flight["departure"].get("estimated"))
                departures.append(dep_time)
                
            if "arrival" in flight and flight["arrival"]:
                # Get scheduled time, then actual, then estimated  
                arr_time = (flight["arrival"].get("actual") or 
                           flight["arrival"].get("scheduled") or 
                           flight["arrival"].get("estimated"))
                arrivals.append(arr_time)

    return {"departures": departures, "arrivals": arrivals}

def export_flights_to_csv(departures, arrivals, date_str):
    """
    Exports flight data to CSV files.
    
    Args:
        departures (list): List of departure flight dictionaries
        arrivals (list): List of arrival flight dictionaries  
        date_str (str): Date string for filename
    """
    
    def extract_flight_data(flights, flight_type):
        """Extract relevant fields from flight data for CSV"""
        csv_data = []
        
        for flight in flights:
            flight_info = flight.get('flight', {})
            airline_info = flight.get('airline', {})
            departure_info = flight.get('departure', {})
            arrival_info = flight.get('arrival', {})
            
            row = {
                'flight_type': flight_type,
                'flight_number': flight_info.get('iata', ''),
                'flight_icao': flight_info.get('icao', ''),
                'airline_name': airline_info.get('name', ''),
                'airline_iata': airline_info.get('iata', ''),
                'flight_status': flight.get('flight_status', ''),
                'flight_date': flight.get('flight_date', ''),
                
                # Departure info
                'dep_airport': departure_info.get('airport', ''),
                'dep_iata': departure_info.get('iata', ''),
                'dep_icao': departure_info.get('icao', ''),
                'dep_terminal': departure_info.get('terminal', ''),
                'dep_gate': departure_info.get('gate', ''),
                'dep_scheduled': departure_info.get('scheduled', ''),
                'dep_estimated': departure_info.get('estimated', ''),
                'dep_actual': departure_info.get('actual', ''),
                'dep_delay': departure_info.get('delay', ''),
                
                # Arrival info  
                'arr_airport': arrival_info.get('airport', ''),
                'arr_iata': arrival_info.get('iata', ''),
                'arr_icao': arrival_info.get('icao', ''),
                'arr_terminal': arrival_info.get('terminal', ''),
                'arr_gate': arrival_info.get('gate', ''),
                'arr_baggage': arrival_info.get('baggage', ''),
                'arr_scheduled': arrival_info.get('scheduled', ''),
                'arr_estimated': arrival_info.get('estimated', ''),
                'arr_actual': arrival_info.get('actual', ''),
                'arr_delay': arrival_info.get('delay', ''),
                
                # Aircraft info
                'aircraft_registration': flight.get('aircraft', {}).get('registration', '') if flight.get('aircraft') else '',
                'aircraft_iata': flight.get('aircraft', {}).get('iata', '') if flight.get('aircraft') else '',
                
                # Codeshare info
                'is_codeshare': 'Yes' if flight_info.get('codeshared') else 'No',
                'codeshare_airline': flight_info.get('codeshared', {}).get('airline_name', '') if flight_info.get('codeshared') else '',
                'codeshare_flight': flight_info.get('codeshared', {}).get('flight_iata', '') if flight_info.get('codeshared') else ''
            }
            csv_data.append(row)
        
        return csv_data
    
    # Extract data for both departures and arrivals
    all_flight_data = []
    
    if departures:
        dep_data = extract_flight_data(departures, 'Departure')
        all_flight_data.extend(dep_data)
    
    if arrivals:
        arr_data = extract_flight_data(arrivals, 'Arrival') 
        all_flight_data.extend(arr_data)
    
    if not all_flight_data:
        print("No flight data to export")
        return
    
    # Create CSV filename
    filename = f"bristol_flights_{date_str}.csv"
    
    # Write to CSV
    fieldnames = [
        'flight_type', 'flight_number', 'flight_icao', 'airline_name', 'airline_iata',
        'flight_status', 'flight_date', 
        'dep_airport', 'dep_iata', 'dep_icao', 'dep_terminal', 'dep_gate',
        'dep_scheduled', 'dep_estimated', 'dep_actual', 'dep_delay',
        'arr_airport', 'arr_iata', 'arr_icao', 'arr_terminal', 'arr_gate', 'arr_baggage',
        'arr_scheduled', 'arr_estimated', 'arr_actual', 'arr_delay',
        'aircraft_registration', 'aircraft_iata', 'is_codeshare', 'codeshare_airline', 'codeshare_flight'
    ]
    
    try:
        with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(all_flight_data)
        
        print(f"\n✅ Flight data exported to: {filename}")
        print(f"📊 Total flights exported: {len(all_flight_data)}")
        print(f"✈️ Departures: {len([f for f in all_flight_data if f['flight_type'] == 'Departure'])}")
        print(f"🛬 Arrivals: {len([f for f in all_flight_data if f['flight_type'] == 'Arrival'])}")
        
    except Exception as e:
        print(f"❌ Error writing CSV file: {e}")

def export_separate_csv_files(departures, arrivals, date_str):
    """
    Alternative: Export departures and arrivals to separate CSV files
    """
    
    def write_csv(flights, flight_type, filename):
        if not flights:
            print(f"No {flight_type.lower()} data to export")
            return
            
        csv_data = []
        for flight in flights:
            flight_info = flight.get('flight', {})
            airline_info = flight.get('airline', {})
            departure_info = flight.get('departure', {})
            arrival_info = flight.get('arrival', {})
            
            row = {
                'flight_number': flight_info.get('iata', ''),
                'airline': airline_info.get('name', ''),
                'status': flight.get('flight_status', ''),
                'from_airport': departure_info.get('airport', ''),
                'to_airport': arrival_info.get('airport', ''),
                'scheduled_time': departure_info.get('scheduled', '') if flight_type == 'Departures' else arrival_info.get('scheduled', ''),
                'actual_time': departure_info.get('actual', '') if flight_type == 'Departures' else arrival_info.get('actual', ''),
                'delay_minutes': departure_info.get('delay', '') if flight_type == 'Departures' else arrival_info.get('delay', ''),
                'terminal': departure_info.get('terminal', '') if flight_type == 'Departures' else arrival_info.get('terminal', ''),
                'gate': departure_info.get('gate', '') if flight_type == 'Departures' else arrival_info.get('gate', '')
            }
            csv_data.append(row)
        
        fieldnames = ['flight_number', 'airline', 'status', 'from_airport', 'to_airport', 
                     'scheduled_time', 'actual_time', 'delay_minutes', 'terminal', 'gate']
        
        try:
            with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(csv_data)
            print(f"✅ {flight_type} exported to: {filename} ({len(csv_data)} flights)")
        except Exception as e:
            print(f"❌ Error writing {filename}: {e}")
    
    # Export to separate files
    dep_filename = f"bristol_departures_{date_str}.csv"
    arr_filename = f"bristol_arrivals_{date_str}.csv"
    
    write_csv(departures, 'Departures', dep_filename)
    write_csv(arrivals, 'Arrivals', arr_filename)

if __name__ == "__main__":
    # Get flights for yesterday
    yesterday_date = get_yesterday()
    print(f"Fetching flight data for: {yesterday_date}")
    
    yesterday_departures = get_flights_from_brs(date=yesterday_date)
    yesterday_arrivals = get_flights_to_brs(date=yesterday_date)

    if yesterday_departures is not None and yesterday_arrivals is not None:
        print(f"\n--- Flights Departing from Bristol Yesterday ({len(yesterday_departures)} flights) ---")
        for flight in yesterday_departures[:5]:  # Show first 5 flights
            flight_info = flight.get('flight', {})
            dep_time = flight.get('departure', {}).get('scheduled', 'N/A')
            print(f"Flight Number: {flight_info.get('iata', 'N/A')}, Departure Time: {dep_time}")

        print(f"\n--- Flights Arriving at Bristol Yesterday ({len(yesterday_arrivals)} flights) ---")
        for flight in yesterday_arrivals[:5]:  # Show first 5 flights
            flight_info = flight.get('flight', {})
            arr_time = flight.get('arrival', {}).get('scheduled', 'N/A')
            print(f"Flight Number: {flight_info.get('iata', 'N/A')}, Arrival Time: {arr_time}")

        # Analyze the data to track flight times
        departures_analysis = analyze_flights(yesterday_departures)
        arrivals_analysis = analyze_flights(yesterday_arrivals)

        print(f"\n--- Departure Times Analysis ({len(departures_analysis['departures'])} flights) ---")
        valid_dep_times = [t for t in departures_analysis["departures"] if t is not None]
        print(f"Valid departure times: {len(valid_dep_times)}")
        if valid_dep_times:
            print(f"Sample times: {valid_dep_times[:3]}")
            
        print(f"\n--- Arrival Times Analysis ({len(arrivals_analysis['arrivals'])} flights) ---")
        valid_arr_times = [t for t in arrivals_analysis["arrivals"] if t is not None]
        print(f"Valid arrival times: {len(valid_arr_times)}")
        if valid_arr_times:
            print(f"Sample times: {valid_arr_times[:3]}")
        
        # Export to CSV files
        print(f"\n--- Exporting Data to CSV ---")
        
        # Option 1: Export all flights to one combined CSV file
        export_flights_to_csv(yesterday_departures, yesterday_arrivals, yesterday_date)
        
        # Option 2: Export to separate CSV files (uncomment if you prefer this)
        # export_separate_csv_files(yesterday_departures, yesterday_arrivals, yesterday_date)
        
    else:
        print("Could not retrieve flight data.")
        print("Check your API key and internet connection.")