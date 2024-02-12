import json
import pandas as pd

json_file_path = 'data/combined_strip.json'

# Load the flight data from JSON file
with open(json_file_path, 'r') as file:
    data = json.load(file)

# Flatten and process the data to include both arrivals and departures
flattened_data = []
for entry in data:
    if 'arrival' in entry and 'actualTime' in entry['arrival'] and entry['arrival']['actualTime']:
        actualTime = entry['arrival']['actualTime']
        if entry['arrival']['iataCode'].lower() == 'brs':  # Capitalize 'BRS'
            flattened_data.append({'actualTime': actualTime, 'type': 'arrival'})
    if 'departure' in entry and 'actualTime' in entry['departure'] and entry['departure']['actualTime']:
        actualTime = entry['departure']['actualTime']
        if entry['departure']['iataCode'].lower() == 'brs':  # Capitalize 'BRS'
            flattened_data.append({'actualTime': actualTime, 'type': 'departure'})

# Convert to DataFrame and format the 'actualTime' to datetime
df = pd.DataFrame(flattened_data)
df['actualTime'] = pd.to_datetime(df['actualTime'])

# Extract year-month from 'actualTime' and count flights per month
df['year_month'] = df['actualTime'].dt.to_period('M')
flight_counts = df.groupby('year_month').size()

# Display the flight counts per month
for year_month, count in flight_counts.items():
    print(f"Month: {year_month}, Number of Flights: {count}")
