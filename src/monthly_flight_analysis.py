import json
from collections import Counter
from datetime import datetime
import pandas as pd

# Define the relative path for the JSON file in the data directory
json_file_path = 'data\\combined_flights_data2.json'

# Load the flight data from JSON file
with open(json_file_path, 'r') as file:
    data = json.load(file)

# Flatten and process the data to include both arrivals and departures
flattened_data = []
for entry in data:
    actualTime = None
    if 'arrival' in entry and 'actualTime' in entry['arrival'] and entry['arrival']['iataCode'] == 'brs':
        actualTime = entry['arrival']['actualTime']
    elif 'departure' in entry and 'actualTime' in entry['departure'] and entry['departure']['iataCode'] == 'brs':
        actualTime = entry['departure']['actualTime']

    if actualTime:
        flattened_data.append({'actualTime': actualTime})

# Convert to DataFrame and format the 'actualTime' to datetime
df = pd.DataFrame(flattened_data)
df['actualTime'] = pd.to_datetime(df['actualTime'])

# Extract month-year from 'actualTime' and count flights per month
flight_dates = df['actualTime'].dt.strftime('%Y-%m')
flight_counts = Counter(flight_dates)

# Display the flight counts per month
for month, count in sorted(flight_counts.items()):
    print(f"Month: {month}, Number of Flights: {count}")





