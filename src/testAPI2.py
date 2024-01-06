import requests
import json
from datetime import datetime, timedelta

# Assuming api_key is imported from your config file
from config import api_key

# Base URL for the Aviationstack API
base_url = "http://api.aviationstack.com/v1/flights"
code = 'EGGD'  # Your airport code

def fetch_all_data(date, is_arrival=True):
    all_data = []
    offset = 0
    limit = 100  # Max limit as per API

    while True:
        params = {
            'access_key': api_key,
            'flight_date': date,
            'limit': limit,
            'offset': offset
        }

        if is_arrival:
            params['arr_icao'] = 'EGGD'
        else:
            params['dep_icao'] = 'EGGD'

        response = requests.get(base_url, params=params)
        if response.status_code == 200:
            data = response.json()
            all_data.extend(data['data'])  # Assuming 'data' is the key for results

            # Check if we have fewer results than the limit, indicating we've reached the last page
            if len(data['data']) < limit:
                break

            offset += limit  # Increase offset for the next request
        else:
            print(f"Error fetching data: {response.status_code}")
            print(response.text)
            break

    return all_data

# Example usage
yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
all_arrivals = fetch_all_data(yesterday, is_arrival=True)
all_departures = fetch_all_data(yesterday, is_arrival=False)
