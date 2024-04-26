import requests
import os
import json  

api_key = "b8cc10-7e0571"

def get_flight_data(api_key, airport_iata, flight_type, date):
    base_url = "https://aviation-edge.com/v2/public/flightsHistory"
    params = {
        'key': api_key,
        'code': airport_iata,
        'type': flight_type,  # 'arrival' or 'departure'
        'date_from': date,
        'date_to': date  # Same date to fetch data for a single day
    }
    response = requests.get(base_url, params=params)
    if response.status_code == 200:
        data = response.json()
        if isinstance(data, dict) and 'error' in data:
            print("Error from API:", data['error'])
            return None
        return data
    else:
        print(f"HTTP Error: {response.status_code}")
        return None

def save_flights_to_json(flights_data, date, airport_iata, flight_type):
    filename = f"{airport_iata}_{flight_type}_flights_{date}.json"
    with open(filename, mode='w') as file:
        json.dump(flights_data, file, indent=4)
    print(f"{flight_type.capitalize()} data saved to {filename}")

#api_key = os.getenv('api_key')  # Ensure the API key is set in your environment variables
airport_iata = 'BRS'  # Bristol Airport
date = '2023-05-31'  # Change date here

# Get departure data for BRS
departure_data = get_flight_data(api_key, airport_iata, 'departure', date)
if departure_data:
    save_flights_to_json(departure_data, date, airport_iata, 'departure')
else:
    print("No departure data retrieved.")

# Get arrival data for BRS
arrival_data = get_flight_data(api_key, airport_iata, 'arrival', date)
if arrival_data:
    save_flights_to_json(arrival_data, date, airport_iata, 'arrival')
else:
    print("No arrival data retrieved.")

