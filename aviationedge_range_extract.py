import requests
import os
import json
from datetime import datetime, timedelta

api_key = "b8cc10-7e0571"

def get_flight_data(api_key, airport_iata, flight_type, date_from, date_to):
    base_url = "https://aviation-edge.com/v2/public/flightsHistory"
    params = {
        'key': api_key,
        'code': airport_iata,
        'type': flight_type,  # 'arrival' or 'departure'
        'date_from': date_from,
        'date_to': date_to
    }
    response = requests.get(base_url, params=params)
    if response.status_code == 200:
        data = response.json()
        if isinstance(data, dict) and 'error' in data:
            print(f"Error from API: {data['error']} for {date_from} to {date_to}")
            return None
        return data
    else:
        print(f"HTTP Error: {response.status_code} for {date_from} to {date_to}")
        return None

def save_flights_to_json(flights_data, date_from, date_to, airport_iata, flight_type):
    filename = f"{airport_iata}_{flight_type}_flights_{date_from}_to_{date_to}.json"
    with open(filename, mode='w') as file:
        json.dump(flights_data, file, indent=4)
    print(f"{flight_type.capitalize()} data from {date_from} to {date_to} saved to {filename}")

def daterange(start_date, end_date):
    for n in range(int((end_date - start_date).days) + 1):
        yield start_date + timedelta(n)

# Set up your date range here
start_date = datetime.strptime('2023-10-02', '%Y-%m-%d')
end_date = datetime.strptime('2023-10-08', '%Y-%m-%d')

airport_iata = 'BRS'  # Bristol Airport BRS -- OTHERS LBA, NCL LGW

# Iterate over each date
for single_date in daterange(start_date, end_date):
    date_str = single_date.strftime('%Y-%m-%d')
    # Get departure data for each date
    departure_data = get_flight_data(api_key, airport_iata, 'departure', date_str, date_str)
    if departure_data:
        save_flights_to_json(departure_data, date_str, date_str, airport_iata, 'departure')
    else:
        print(f"No departure data retrieved for {date_str}.")

    # Get arrival data for each date
    arrival_data = get_flight_data(api_key, airport_iata, 'arrival', date_str, date_str)
    if arrival_data:
        save_flights_to_json(arrival_data, date_str, date_str, airport_iata, 'arrival')
    else:
        print(f"No arrival data retrieved for {date_str}.")
