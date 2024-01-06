import requests
import json
from datetime import datetime, timedelta

# Assuming api_key is imported from your config file
from config import api_key

# Base URL for the Aviationstack API
base_url = "http://api.aviationstack.com/v1/flights"
code = 'EGGD'  # Your airport code

def fetch_data(date, is_arrival=True, filename=None):
    params = {
        'access_key': api_key,
        'flight_date': date
    }

    if is_arrival:
        params['arr_icao'] = 'EGGD'  # For flights arriving at EGGD
    else:
        params['dep_icao'] = 'EGGD'  # For flights departing from EGGD

    response = requests.get(base_url, params=params)
    if response.status_code == 200:
        data = response.json()
        if filename:
            with open(filename, 'w') as file:
                json.dump(data, file)
            print(f"Data saved to {filename}")
        return data
    else:
        print(f"Error fetching data for {date}: {response.status_code}")
        print(response.text)
        return None


# Get yesterday's date
yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

# Fetch and save arrival data
arrival_filename = f'EGGD_arrivals_{yesterday}.json'
fetch_data(yesterday, is_arrival=True, filename=arrival_filename)

# Fetch and save departure data
departure_filename = f'EGGD_departures_{yesterday}.json'
fetch_data(yesterday, is_arrival=False, filename=departure_filename)


# Fetch data for yesterday
print(f"Fetching data for {yesterday}")
yesterday_data = fetch_data(yesterday)

import json

# Replace 'YYYY-MM-DD' with the actual date
arrival_filename = f'EGGD_arrivals_{yesterday}.json'
departure_filename = f'EGGD_departures_{yesterday}.json'
combined_filename = f'EGGD_combined_{yesterday}.json'

try:
    # Read arrival data
    with open(arrival_filename, 'r') as file:
        arrivals = json.load(file)

    # Read departure data
    with open(departure_filename, 'r') as file:
        departures = json.load(file)

    # Combine the data
    combined_data = {
        'arrivals': arrivals,
        'departures': departures
    }

    # Write the combined data to a new file
    with open(combined_filename, 'w') as file:
        json.dump(combined_data, file)

    print(f"Combined data saved to {combined_filename}")

except FileNotFoundError as e:
    print(f"Error: {e}")

