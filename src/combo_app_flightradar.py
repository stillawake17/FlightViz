import requests
import json
from datetime import datetime, timedelta, timezone
import logging
import os

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

# Assuming your API key is stored in config.py
from config import FR24_API_KEY  

# Base URL for the FlightRadar24 API
base_url = "https://fr24api.flightradar24.com"

# Function to convert timestamp to a human-readable date
def timestamp_to_date(timestamp):
    try:
        # Convert the timestamp to an integer
        timestamp = int(timestamp)
        dt_object = datetime.fromtimestamp(timestamp, tz=timezone.utc)
        return dt_object.strftime("%Y-%m-%d %H:%M:%S %Z")
    except (ValueError, TypeError):
        return 'Invalid Timestamp'

# Function to fetch and count flights from FlightRadar24 API for Bristol bounds with pagination and reporting on the timestamp
def fetch_flight_data_with_timestamp(timestamp):
    # Convert the timestamp to a human-readable date
    formatted_date = timestamp_to_date(timestamp)
    logging.info(f"Fetching flight data for date: {formatted_date}")

    # Set the API token in headers
    headers = {
        'Accept': 'application/json',
        'Accept-Version': 'v1',
        'Authorization': f'Bearer {FR24_API_KEY}'
    }

    # Define bounds for Bristol (Latitude: 50.5 to 52.0, Longitude: -4.0 to -1.5)
    bounds = "52.0,50.5,-4.0,-1.5"

    all_flights = []  # List to store all flights
    limit = 100  # Number of results per page (adjust based on API limits)
    offset = 0  # Initial offset
    max_pages = 10  # To prevent infinite loops
    current_page = 0

    while current_page < max_pages:
        # API endpoint with pagination
        endpoint = f"{base_url}/api/historic/flight-positions/full"
        params = {
            'bounds': bounds,
            'timestamp': timestamp,
            'limit': limit,
            'offset': offset
        }

        try:
            response = requests.get(endpoint, headers=headers, params=params)
            response.raise_for_status()  # Raises HTTPError if the status is 4xx or 5xx
            data = response.json()

            # Access the 'data' key where the flights are stored
            flights = data.get('data', [])

            if not flights:
                # Break if no more flights are returned
                break

            # Add the flights to the all_flights list
            all_flights.extend(flights)

            # Increment the offset for the next page
            offset += limit
            current_page += 1

            # Log the number of flights retrieved so far
            logging.info(f"Flights fetched so far: {len(all_flights)}")

        except requests.exceptions.RequestException as e:
            logging.error(f"Error fetching data: {e}")
            break
        except json.JSONDecodeError:
            logging.error("Error decoding JSON response")
            break

    # Filter flights specifically to/from Bristol Airport (EGGD/BRS)
    bristol_flights = []
    for flight in all_flights:
        orig_icao = flight.get('orig_icao')
        orig_iata = flight.get('orig_iata')
        dest_icao = flight.get('dest_icao')
        dest_iata = flight.get('dest_iata')

        if any([
            orig_icao == 'EGGD',
            orig_iata == 'BRS',
            dest_icao == 'EGGD',
            dest_iata == 'BRS'
        ]):
            bristol_flights.append(flight)

    # Log the total number of flights specifically to/from Bristol Airport
    logging.info(f"Total number of flights to/from Bristol Airport (EGGD/BRS): {len(bristol_flights)}")

    # Print summary details including the timestamp
    if bristol_flights:
        logging.info("\nBristol Airport Flights Summary:")
        for flight in bristol_flights:
            category = flight.get('categories', 'N/A')
            flight_number = flight.get('flight', 'N/A')
            orig_icao = flight.get('orig_icao')
            orig_iata = flight.get('orig_iata')
            dest_icao = flight.get('dest_icao')
            dest_iata = flight.get('dest_iata')
            timestamp_flight = flight.get('timestamp', 'N/A')
            lat = flight.get('lat', 'N/A')
            lon = flight.get('lon', 'N/A')

            # Process the timestamp
            if timestamp_flight and timestamp_flight != 'N/A':
                try:
                    flight_time = timestamp_to_date(timestamp_flight)
                except (ValueError, TypeError):
                    flight_time = 'Invalid Timestamp'
            else:
                flight_time = 'N/A'

            orig_info = f"{orig_icao}/{orig_iata}" if orig_icao or orig_iata else 'N/A'
            dest_info = f"{dest_icao}/{dest_iata}" if dest_icao or dest_iata else 'N/A'

            logging.info(
                f"Flight {flight_number} (Category: {category}): "
                f"Orig {orig_info}, Dest {dest_info}, "
                f"Time at Position: {flight_time}, Lat: {lat}, Lon: {lon}"
            )

# Get the timestamp for two days ago in UTC
two_days_ago = datetime.now(timezone.utc) - timedelta(days=2)
two_days_ago_timestamp = int(two_days_ago.timestamp())

# Fetch and count flight data specifically for Bristol Airport, reporting on the timestamp
fetch_flight_data_with_timestamp(two_days_ago_timestamp)
