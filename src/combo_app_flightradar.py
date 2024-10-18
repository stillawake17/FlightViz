import http.client
import json
from datetime import datetime, timedelta

# Assuming your API key is stored in a separate config file
from config import api_key  

# Base URL for the FlightRadar24 API
base_url = "fr24api.flightradar24.com"

# Function to convert timestamp to a human-readable date
def timestamp_to_date(timestamp):
    dt_object = datetime.fromtimestamp(timestamp)
    return dt_object.strftime("%Y-%m-%d")

# Function to fetch and count flights from FlightRadar24 API for Bristol bounds with pagination and reporting on the timestamp
def fetch_flight_data_with_timestamp(timestamp):
    # Convert the timestamp to a human-readable date
    formatted_date = timestamp_to_date(timestamp)
    print(f"Fetching flight data for date: {formatted_date}")

    # Establish the connection to FlightRadar24 API
    conn = http.client.HTTPSConnection(base_url)

    # Set the API token in headers
    headers = {
        'Accept': 'application/json',
        'Accept-Version': 'v1',
        'Authorization': f'Bearer {api_key}'
    }

    # Define bounds for Bristol (Latitude: 50.5 to 52.0, Longitude: -4.0 to -1.5)
    bounds = "52.0,50.5,-4.0,-1.5"

    all_flights = []  # List to store all flights
    limit = 100  # Number of results per page (adjust based on API limits)
    offset = 0  # Initial offset

    while True:
        # API endpoint with pagination
        endpoint = f"/api/historic/flight-positions/full?bounds={bounds}&timestamp={timestamp}&limit={limit}&offset={offset}"
        conn.request("GET", endpoint, '', headers)
        response = conn.getresponse()

        if response.status == 200:
            data = json.loads(response.read().decode("utf-8"))

            # Access the 'data' key where the flights are stored
            flights = data.get('data', [])

            if not flights:
                # Break if no more flights are returned
                break

            # Add the flights to the all_flights list
            all_flights.extend(flights)

            # Increment the offset for the next page
            offset += limit

            # Print out the number of flights retrieved so far
            print(f"Flights fetched so far: {len(all_flights)}")

        else:
            print(f"Error fetching data for timestamp {timestamp}: {response.status}")
            print(response.read().decode("utf-8"))
            break

    # Filter flights specifically to/from Bristol Airport (EGGD/BRS)
    bristol_flights = [
        flight for flight in all_flights
        if flight.get('orig_icao') == 'EGGD' or flight.get('orig_iata') == 'BRS' or
           flight.get('dest_icao') == 'EGGD' or flight.get('dest_iata') == 'BRS'
    ]

    # Print out the total number of flights specifically to/from Bristol Airport
    print(f"\nTotal number of flights to/from Bristol Airport (EGGD/BRS): {len(bristol_flights)}")

    # Print summary details including the timestamp
    if len(bristol_flights) > 0:
        print("\nBristol Airport Flights Summary (with timestamp):")
        for flight in bristol_flights:
            category = flight.get('categories', 'N/A')  # Access the 'categories' field
            timestamp = flight.get('timestamp', 'N/A')  # Get the 'timestamp' field
            print(f"Flight {flight.get('flight', 'N/A')} (Category: {category}): "
                  f"Orig {flight.get('orig_icao', 'N/A')}/{flight.get('orig_iata', 'N/A')}, "
                  f"Dest {flight.get('dest_icao', 'N/A')}/{flight.get('dest_iata', 'N/A')}, "
                  f"Time at Bristol: {timestamp}, Lat: {flight.get('lat')}, Lon: {flight.get('lon')}")

# Get the timestamp for two days ago
two_days_ago_timestamp = int((datetime.now() - timedelta(days=2)).timestamp())

# Fetch and count flight data specifically for Bristol Airport, reporting on the timestamp
fetch_flight_data_with_timestamp(two_days_ago_timestamp)
