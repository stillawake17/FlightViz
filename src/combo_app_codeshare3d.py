def process_codeshare_flights(self, flights, debug_codeshares=True):
    """
    Process flights with correct Bristol-specific time categorization
    """
    processed_flights = []
    seen_flights = set()
    codeshare_count = 0
    
    print(f"\n=== Processing {len(flights)} flights ===")
    
    for i, flight in enumerate(flights):
        flight_info = flight.get('flight', {})
        codeshared = flight_info.get('codeshared')
        flight_number = flight_info.get('number')
        
        # Get arrival and departure info
        departure_info = flight.get('departure', {})
        arrival_info = flight.get('arrival', {})
        
        # Create flight signature based on time and route
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
        
        # Skip duplicates
        if flight_signature in seen_flights:
            if debug_codeshares:
                print(f"Skipping duplicate flight: {flight_number}")
            continue
        
        seen_flights.add(flight_signature)
        
        # CRITICAL: Determine if this is an arrival TO Bristol or departure FROM Bristol
        is_bristol_arrival = arrival_info.get('icao') == self.airport_code
        is_bristol_departure = departure_info.get('icao') == self.airport_code
        
        # Debug info for first few flights
        if i < 5:
            print(f"\nFlight {flight_number}:")
            print(f"  Route: {departure_info.get('airport', 'Unknown')} ({departure_info.get('icao', '?')}) -> "
                  f"{arrival_info.get('airport', 'Unknown')} ({arrival_info.get('icao', '?')})")
            print(f"  Is Bristol arrival: {is_bristol_arrival}")
            print(f"  Is Bristol departure: {is_bristol_departure}")
        
        # Get the BRISTOL-specific time
        bristol_time = None
        movement_type = None
        
        if is_bristol_arrival:
            # For arrivals TO Bristol, use Bristol arrival time
            movement_type = 'arrival'
            bristol_time = (arrival_info.get('actual') or 
                          arrival_info.get('estimated') or 
                          arrival_info.get('scheduled'))
            if i < 5:
                print(f"  Using Bristol ARRIVAL time: {bristol_time}")
                
        elif is_bristol_departure:
            # For departures FROM Bristol, use Bristol departure time
            movement_type = 'departure'
            bristol_time = (departure_info.get('actual') or 
                          departure_info.get('estimated') or 
                          departure_info.get('scheduled'))
            if i < 5:
                print(f"  Using Bristol DEPARTURE time: {bristol_time}")
        else:
            # This shouldn't happen if data is filtered correctly
            print(f"WARNING: Flight {flight_number} doesn't involve Bristol!")
            continue
        
        # Categorize based on Bristol time
        flight['time_category'] = self.categorize_flight(bristol_time)
        flight['bristol_time'] = bristol_time  # Store for debugging
        flight['movement_type'] = movement_type  # Store whether arrival or departure
        
        if i < 5:
            print(f"  Category: {flight['time_category']}")
        
        processed_flights.append(flight)
    
    print(f"\nProcessed {len(processed_flights)} unique flights from {len(flights)} raw flights")
    print(f"Found {codeshare_count} codeshare flights")
    
    # Summary of categorization
    category_counts = {}
    for flight in processed_flights:
        cat = flight.get('time_category', 'Unknown')
        category_counts[cat] = category_counts.get(cat, 0) + 1
    
    print(f"\nCategorization summary:")
    for cat, count in category_counts.items():
        print(f"  {cat}: {count}")
    
    return processed_flights


def categorize_flight(self, time_str):
    """
    Categorize flight based on Bristol arrival/departure time
    Night hours: 23:30-05:59
    Shoulder hour: 06:00-06:59 and 23:00-23:29
    Regular: All other times
    """
    if not time_str:
        return "Unknown"
        
    try:
        # Handle ISO format with timezone
        if 'T' in time_str:
            if time_str.endswith('Z'):
                time_str = time_str[:-1] + '+00:00'
            dt = datetime.fromisoformat(time_str)
        else:
            # For simple time format, parse the time
            dt = datetime.strptime(time_str, '%H:%M')
        
        hour = dt.hour
        minute = dt.minute
        
        # Create a time value for easier comparison
        time_value = hour * 100 + minute  # e.g., 23:30 becomes 2330
        
        # Night hours: 23:30-05:59
        if time_value >= 2330 or time_value < 600:
            return "Night hour flights"
        # Shoulder hours: 06:00-06:59 and 23:00-23:29
        elif 600 <= time_value < 700 or 2300 <= time_value < 2330:
            return "Shoulder hour flights"
        # Regular hours: 07:00-22:59
        else:
            return "Regular flights"
            
    except (ValueError, TypeError) as e:
        print(f"Error parsing time '{time_str}': {e}")
        return "Unknown"


def generate_detailed_summary(self, data):
    """Generate summary with Bristol-specific movement details"""
    categories = {
        'Regular flights': 0,
        'Shoulder hour flights': 0,
        'Night hour flights': 0,
        'Unknown': 0
    }
    
    # Separate tracking for arrivals and departures
    arrival_categories = {
        'Regular flights': 0,
        'Shoulder hour flights': 0,
        'Night hour flights': 0,
        'Unknown': 0
    }
    
    departure_categories = {
        'Regular flights': 0,
        'Shoulder hour flights': 0,
        'Night hour flights': 0,
        'Unknown': 0
    }
    
    # Track night flight details
    night_airlines = {}
    night_routes_arrivals = {}  # Where night arrivals come FROM
    night_routes_departures = {}  # Where night departures go TO
    
    # Process arrivals
    for flight in data['arrivals']:
        category = flight.get('time_category', 'Unknown')
        categories[category] += 1
        arrival_categories[category] += 1
        
        if category == 'Night hour flights':
            # Track airline
            airline = flight.get('airline', {}).get('name', 'Unknown')
            night_airlines[airline] = night_airlines.get(airline, 0) + 1
            
            # Track where the flight came FROM
            origin = flight.get('departure', {}).get('airport', 'Unknown')
            night_routes_arrivals[origin] = night_routes_arrivals.get(origin, 0) + 1
    
    # Process departures
    for flight in data['departures']:
        category = flight.get('time_category', 'Unknown')
        categories[category] += 1
        departure_categories[category] += 1
        
        if category == 'Night hour flights':
            # Track airline
            airline = flight.get('airline', {}).get('name', 'Unknown')
            night_airlines[airline] = night_airlines.get(airline, 0) + 1
            
            # Track where the flight goes TO
            destination = flight.get('arrival', {}).get('airport', 'Unknown')
            night_routes_departures[destination] = night_routes_departures.get(destination, 0) + 1
    
    summary = {
        'total_flights': len(data['arrivals']) + len(data['departures']),
        'arrivals': len(data['arrivals']),
        'departures': len(data['departures']),
        'categories': categories,
        'arrival_categories': arrival_categories,
        'departure_categories': departure_categories,
        'night_flights': {
            'total': categories.get('Night hour flights', 0),
            'arrivals': arrival_categories.get('Night hour flights', 0),
            'departures': departure_categories.get('Night hour flights', 0),
            'airlines': night_airlines,
            'arrival_origins': night_routes_arrivals,
            'departure_destinations': night_routes_departures
        }
    }
    
    # Print detailed summary
    print("\n=== BRISTOL AIRPORT MOVEMENT SUMMARY ===")
    print(f"Total movements: {summary['total_flights']}")
    print(f"  Arrivals to Bristol: {summary['arrivals']}")
    print(f"  Departures from Bristol: {summary['departures']}")
    
    print(f"\nMovements by time category:")
    for cat, count in categories.items():
        print(f"  {cat}: {count}")
    
    print(f"\nNight movements (23:30-05:59):")
    print(f"  Total: {summary['night_flights']['total']}")
    print(f"  Arrivals to Bristol: {summary['night_flights']['arrivals']}")
    print(f"  Departures from Bristol: {summary['night_flights']['departures']}")
    
    if night_routes_arrivals:
        print(f"\n  Night arrivals come FROM:")
        for origin, count in sorted(night_routes_arrivals.items(), key=lambda x: x[1], reverse=True):
            print(f"    {origin}: {count}")
    
    if night_routes_departures:
        print(f"\n  Night departures go TO:")
        for dest, count in sorted(night_routes_departures.items(), key=lambda x: x[1], reverse=True):
            print(f"    {dest}: {count}")
    
    return summary