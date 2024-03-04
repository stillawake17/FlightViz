import json
import pandas as pd

json_file_path = 'data/combined_strip.json'
#json_file_path = 'deduplicated_data_2024-02-22.json'

# Load the flight data from JSON file
with open(json_file_path, 'r') as file:
    data = json.load(file)

# Flatten and process the data to include both arrivals and departures
flattened_data = []
for entry in data:
    if 'arrival' in entry and 'actualTime' in entry['arrival'] and entry['arrival']['actualTime']:
        actualTime = entry['arrival']['actualTime']
        if entry['arrival']['iataCode'].lower() == 'brs':  # lower case to match the api
            flattened_data.append({'actualTime': actualTime, 'type': 'arrival'})
    if 'departure' in entry and 'actualTime' in entry['departure'] and entry['departure']['actualTime']:
        actualTime = entry['departure']['actualTime']
        if entry['departure']['iataCode'].lower() == 'brs':  # lower case to match the api
            flattened_data.append({'actualTime': actualTime, 'type': 'departure'})

# Convert to DataFrame and format the 'actualTime' to datetime
df = pd.DataFrame(flattened_data)
df['actualTime'] = pd.to_datetime(df['actualTime'])

# Extract year-month and date from 'actualTime'
df['year_month'] = df['actualTime'].dt.to_period('M')
df['date'] = df['actualTime'].dt.date

# Count flights per month
monthly_counts = df.groupby('year_month').size().reset_index(name='count')
# Count flights per day
daily_counts = df.groupby('date').size().reset_index(name='count')

# Export counts to CSV
monthly_counts.to_csv('monthly_flight_counts.csv', index=False)
daily_counts.to_csv('daily_flight_counts.csv', index=False)

print("Monthly and daily flight counts have been exported to CSV files.")

import json

# Assuming 'df' is your DataFrame after the preprocessing steps

# Filter for June, July, and August 2023
df_filtered = df[df['actualTime'].dt.year == 2023]
df_filtered = df_filtered[df_filtered['actualTime'].dt.month.isin([6, 7, 8])]

# Define night-time as between 11:30 PM and less than 6 AM
df_night = df_filtered[((df_filtered['actualTime'].dt.hour == 23) & (df_filtered['actualTime'].dt.minute >= 30)) | 
                       (df_filtered['actualTime'].dt.hour < 6) |
                       (df_filtered['actualTime'].dt.hour == 0)]

# Group by type and count
night_counts = df_night.groupby('type').size().reset_index(name='night_count')

# Convert to dictionary and then to JSON
night_counts_dict = night_counts.to_dict(orient='records')
night_counts_json = json.dumps(night_counts_dict)

print(night_counts_json)

# Optionally, save the JSON data to a file
with open('night_time_flight_counts.json', 'w') as json_file:
    json_file.write(night_counts_json)

print("Night-time flight counts for June, July, and August 2023 have been saved to JSON file.")
