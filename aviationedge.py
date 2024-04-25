
import requests
import os
import csv
from datetime import datetime

api_key = "b8cc10-7e0571"

def get_flights(api_key, airport_iata):
    base_url = "https://aviation-edge.com/v2/public/flights"
    params = {
        'key': api_key,
        'arrIata': airport_iata,
    }
    response = requests.get(base_url, params=params)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error: {response.status_code}")
        return None

def save_flights_to_csv(flights_data):
    headers = ['flightId', 'airline', 'arrival_time', 'status']
    flights_by_date = {}

    for flight in flights_data:
        flightId = flight.get('flight', {}).get('iataNumber', '')
        airline = flight.get('airline', {}).get('iataCode', '')
        status = flight.get('status', '')
        updated_timestamp = flight.get('system', {}).get('updated', None)

        if updated_timestamp:
            # Convert the Unix timestamp to a datetime object
            date_time = datetime.utcfromtimestamp(updated_timestamp)
            date_str = date_time.strftime('%Y-%m-%d')
            time_str = date_time.strftime('%H:%M:%S')
        else:
            date_str = 'unknown_date'
            time_str = 'unknown_time'

        # Append flight to the correct date
        if date_str not in flights_by_date:
            flights_by_date[date_str] = []
        flights_by_date[date_str].append([flightId, airline, time_str, status])

    # Write each day's flights to a separate CSV file
    for date, flights in flights_by_date.items():
        filename = f"flights_{date}.csv"
        with open(filename, mode='w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(headers)  # Write headers
            writer.writerows(flights)  # Write all flights for the date

api_key = os.getenv('api_key')  # Make sure your API key is set in your environment variables
airport_iata = 'BRS'  # IATA code for Bristol Airport

# Get flight data
flights_data = get_flights(api_key, airport_iata)
if flights_data:
    save_flights_to_csv(flights_data)
    print("Data saved successfully.")
else:
    print("No data retrieved.")