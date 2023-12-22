import json
from collections import Counter
from datetime import datetime

# Define the relative path for the JSON file in the data directory
# json_file_path = '../data/bristol_airport_data.json'
#json_file_path = 'data\\bristol_airport_2023-12-21_data.json'
json_file_path = 'data\\combined_flights_data.json'


# Load the flight data from JSON file
with open(json_file_path, 'r') as file:
    data = json.load(file)

# Extract dates from the data
# Switching to 'actualTime' instead of 'scheduledTime'
flight_dates = [datetime.strptime(flight['departure']['actualTime'], '%Y-%m-%dT%H:%M:%S.%f').strftime('%Y-%m') 
                for flight in data 
                if 'departure' in flight and 'actualTime' in flight['departure']]

# Count flights per month
flight_counts = Counter(flight_dates)

# Display the flight counts per month
for month, count in sorted(flight_counts.items()):
    print(f"Month: {month}, Number of Flights: {count}")
