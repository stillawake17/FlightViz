import json
import pandas as pd

json_file_path = 'combined.json'

# Load the flight data from JSON file
with open(json_file_path, 'r') as file:
    data = json.load(file)

# Preliminary analysis
iata_codes_arrival = [entry['arrival']['iataCode'] for entry in data if 'arrival' in entry and 'iataCode' in entry['arrival']]
iata_codes_departure = [entry['departure']['iataCode'] for entry in data if 'departure' in entry and 'iataCode' in entry['departure']]

# Count occurrences of 'BRS'
arrival_brs_count = iata_codes_arrival.count('BRS')
departure_brs_count = iata_codes_departure.count('BRS')

print(f"Total 'BRS' in arrival iataCode: {arrival_brs_count}")
print(f"Total 'BRS' in departure iataCode: {departure_brs_count}")

# Date range check before filtering
all_dates = [entry['arrival']['actualTime'] for entry in data if 'arrival' in entry and 'actualTime' in entry['arrival']]
all_dates += [entry['departure']['actualTime'] for entry in data if 'departure' in entry and 'actualTime' in entry['departure']]

# Convert to datetime and find the range
all_dates = pd.to_datetime(all_dates, errors='coerce')
print(f"Date range in data: {all_dates.min()} to {all_dates.max()}")

# (Continue with your existing data processing and analysis code...)


# Check the total number of records in the JSON data
print(f"Total records in JSON: {len(data)}")

# Flatten and process the data to include both arrivals and departures
flattened_data = []
for entry in data:
    if 'arrival' in entry and 'actualTime' in entry['arrival'] and entry['arrival']['actualTime'] and entry['arrival']['iataCode'] == 'BRS':
        actualTime = entry['arrival']['actualTime']
        flattened_data.append({'actualTime': actualTime, 'type': 'arrival'})
    elif 'departure' in entry and 'actualTime' in entry['departure'] and entry['departure']['actualTime'] and entry['departure']['iataCode'] == 'BRS':
        actualTime = entry['departure']['actualTime']
        flattened_data.append({'actualTime': actualTime, 'type': 'departure'})

# Check the number of records after processing
print(f"Records after processing: {len(flattened_data)}")

# Convert to DataFrame and format the 'actualTime' to datetime
df = pd.DataFrame(flattened_data)
df['actualTime'] = pd.to_datetime(df['actualTime'])

# Extract year-month from 'actualTime' and count flights per month
df['year_month'] = df['actualTime'].dt.to_period('M')
flight_counts = df.groupby('year_month').size()

# Display the flight counts per month
for year_month, count in flight_counts.items():
    print(f"Month: {year_month}, Number of Flights: {count}")
