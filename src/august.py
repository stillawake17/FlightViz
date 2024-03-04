import json
import pandas as pd

# Path to your JSON file
json_file_path = 'data/combined_strip.json'

# Load the flight data from JSON file
with open(json_file_path, 'r') as file:
    data = json.load(file)

# Helper function to check if time is within the specified night-time range
def is_night_time(actual_time):
    if actual_time is None:
        return False
    time = pd.to_datetime(actual_time)
    # Adjusted for 11:30 PM to less than 6 AM
    if (time.hour == 23 and time.minute >= 30) or (0 <= time.hour < 6):
        return True
    return False

# Process data
bristol_flights = []
for entry in data:
    actual_time = None
    if entry.get('type') == 'arrival' and entry['arrival'].get('icaoCode', '').upper() == 'EGGD' and entry.get('status') == 'landed':
        actual_time = entry['arrival'].get('actualTime')
    elif entry.get('type') == 'departure' and entry['departure'].get('icaoCode', '').upper() == 'EGGD':
        # Assuming departures do not have a "landed" status, so no check for status here
        actual_time = entry['departure'].get('actualTime')
    
    # Check if actual_time is not None and is within the night-time range
    if actual_time and is_night_time(actual_time):
        date = pd.to_datetime(actual_time).date()
        if date.month in [6, 7, 8] and date.year == 2023:
            flight_info = {
                "type": entry['type'],
                "from_icaoCode": entry['departure'].get('icaoCode') if entry.get('type') == 'arrival' else entry['arrival'].get('icaoCode'),
                "to_icaoCode": entry['arrival'].get('icaoCode') if entry.get('type') == 'arrival' else entry['departure'].get('icaoCode'),
                "actualTime": actual_time,
                "status": entry.get('status')
            }
            # Add the flight if it is an arrival with status 'landed' or any departure
            if entry.get('type') == 'arrival' or (entry.get('type') == 'departure' and flight_info not in bristol_flights):
                bristol_flights.append(flight_info)

# Optionally, print or save the filtered list of flights
print(json.dumps(bristol_flights, indent=4))

# If you want to save this data to a new JSON file:
with open('bristol_flights_jun_jul_aug_2023.json', 'w') as outfile:
    json.dump(bristol_flights, outfile, indent=4)

print("Filtered flight data related to Bristol for June, July, and August 2023 has been saved to 'bristol_flights_jun_jul_aug_2023.json'.")

