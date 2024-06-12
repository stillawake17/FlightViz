import requests
import json
from datetime import datetime, timedelta
from config import api_key  # Assuming your API key is stored in a separate config file

base_url = "http://api.aviationstack.com/v1/flights"
code = 'EGGD'  # Your airport code

def fetch_data(date, is_arrival=True, filename=None):
    all_data = []  # Initialize an empty list to hold all records
    params = {
        'access_key': api_key,
        'flight_date': date,
        'limit': 100,  # Adjust limit based on API documentation
        'offset': 0  # Initially start at 0
    }

    if is_arrival:
        params['arr_icao'] = code  # For flights arriving at the specified code
    else:
        params['dep_icao'] = code  # For flights departing from the specified code

    while True:
        response = requests.get(base_url, params=params)
        print(f"Request URL: {response.url}")
        if response.status_code == 200:
            data = response.json()
            print(f"Response data: {json.dumps(data, indent=4)}")
            all_data.extend(data.get('data', []))  # Append the fetched records to all_data list
            
            # Check if we fetched fewer records than the limit, indicating we are done
            if len(data.get('data', [])) < params['limit']:
                break
            
            params['offset'] += params['limit']  # Increment the offset to fetch the next batch
        else:
            print(f"Error fetching data for {date}: {response.status_code}")
            print(response.text)
            break  # Exit the loop in case of an error

    if filename:
        with open(filename, 'w') as file:
            json.dump(all_data, file)  # Save all fetched data to the specified file
        print(f"Data saved to {filename}")

    return all_data  # Return the combined data for further processing

# Get yesterday's date by deleting one day in days=1
yesterday = (datetime.now() - timedelta(days=6)).strftime("%Y-%m-%d")

# Fetch and save arrival data
arrival_filename = f'EGGD_arrivals_{yesterday}.json'
fetch_data(yesterday, is_arrival=True, filename=arrival_filename)

# Fetch and save departure data
departure_filename = f'EGGD_departures_{yesterday}.json'
fetch_data(yesterday, is_arrival=False, filename=departure_filename)
