import requests
import json
from datetime import datetime, timedelta
from pymongo import MongoClient
from config import api_key, mongo_uri

base_url = "http://api.aviationstack.com/v1/flights"
code = 'EGGD'  # Your airport code

# Connect to MongoDB
client = MongoClient(mongo_uri)
db = client['flight_data']
collection = db['daily_flights']

def fetch_data(date, is_arrival=True):
    all_data = []
    params = {
        'access_key': api_key,
        'flight_date': date,
        'limit': 100,
        'offset': 0
    }

    if is_arrival:
        params['arr_icao'] = code
    else:
        params['dep_icao'] = code

    while True:
        response = requests.get(base_url, params=params)
        if response.status_code == 200:
            data = response.json()
            all_data.extend(data.get('data', []))
            if len(data.get('data', [])) < params['limit']:
                break
            params['offset'] += params['limit']
        else:
            print(f"Error fetching data for {date}: {response.status_code}")
            print(response.text)
            break

    return all_data

def insert_data(date, arrivals, departures):
    document = {
        'date': date,
        'arrivals': arrivals,
        'departures': departures
    }
    collection.insert_one(document)
    print(f"Data for {date} inserted into MongoDB.")

def main():
    yesterday = (datetime.now() - timedelta(days=2)).strftime("%Y-%m-%d")
    arrival_data = fetch_data(yesterday, is_arrival=True)
    departure_data = fetch_data(yesterday, is_arrival=False)
    
    insert_data(yesterday, arrival_data, departure_data)

if __name__ == "__main__":
    main()
