import requests
import json
from datetime import datetime, timedelta

# Assuming api_key is imported from your config file
from config import api_key

# Base URL for the Aviationstack API
base_url = "http://api.aviationstack.com/v1/flights"
code = 'EGGD'  # Your airport code

def fetch_data(date, is_arrival=True, filename=None):
    params = {
        'access_key': api_key,
        'flight_date': date,
        'limit': 100,  # adjust limit based on API documentation
        'offset': 100    # this tells the api at which record to start; begin at 0
    }

    if is_arrival:
        params['arr_icao'] = 'EGGD'  # For flights arriving at EGGD
    else:
        params['dep_icao'] = 'EGGD'  # For flights departing from EGGD

    response = requests.get(base_url, params=params)
    if response.status_code == 200:
        data = response.json()
        if filename:
            with open(filename, 'w') as file:
                json.dump(data, file)
            print(f"Data saved to {filename}")
        return data
    else:
        print(f"Error fetching data for {date}: {response.status_code}")
        print(response.text)
        return None


# Get yesterday's date by deleting one day in days=1
yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

# Fetch and save arrival data
arrival_filename = f'EGGD_arrivals_{yesterday}.json'
fetch_data(yesterday, is_arrival=True, filename=arrival_filename)

# Fetch and save departure data
departure_filename = f'EGGD_departures_{yesterday}.json'
fetch_data(yesterday, is_arrival=False, filename=departure_filename)


# Fetch data for yesterday
print(f"Fetching data for {yesterday}")
yesterday_data = fetch_data(yesterday)

import json

# Replace 'YYYY-MM-DD' with the actual date
arrival_filename = f'EGGD_arrivals_{yesterday}.json'
departure_filename = f'EGGD_departures_{yesterday}.json'
combined_filename = f'EGGD_combined_{yesterday}.json'

try:
    # Read arrival data
    with open(arrival_filename, 'r') as file:
        arrivals = json.load(file)

    # Read departure data
    with open(departure_filename, 'r') as file:
        departures = json.load(file)

    # Combine the data
    combined_data = {
        'arrivals': arrivals,
        'departures': departures
    }

    # Write the combined data to a new file
    with open(combined_filename, 'w') as file:
        json.dump(combined_data, file)

    print(f"Combined data saved to {combined_filename}")

except FileNotFoundError as e:
    print(f"Error: {e}")


#Run convert.py after app.py -- this converts the json file to the api format used on the dashboard
import json
from datetime import datetime

# Path to the file containing the new API JSON response
input_file_path = f'EGGD_combined_{yesterday}.json'
# Path where the converted data will be saved
output_file_path = f'converted_{yesterday}.json'

# Read the new API data from a file
with open(input_file_path, 'r') as file:
    new_api_data = json.load(file)

# Print the top-level keys in the JSON file
print("Top-level keys in the JSON file:", new_api_data.keys())

def convert_time_format(time_str):
    if time_str:
        # Parse the datetime from the new format
        dt = datetime.fromisoformat(time_str)
        # Format it to the old format (without timezone information)
        return dt.strftime("%Y-%m-%dt%H:%M:%S.000")
    return ""

def convert_to_old_api_format(flight_data, flight_type):
    old_api_format = []
    for flight in flight_data:
        flight_status = flight.get("flight_status", "unknown")
        departure_info = flight.get("departure", {})
        arrival_info = flight.get("arrival", {})

        old_api_flight = {
            "type": flight_type,
            "status": flight_status,
            "departure": {
                "iataCode": departure_info.get("iata", ""),
                "icaoCode": departure_info.get("icao", ""),
                "actualTime": convert_time_format(departure_info.get("actual", ""))
            },
            "arrival": {
                "iataCode": arrival_info.get("iata", ""),
                "icaoCode": arrival_info.get("icao", ""),
                "actualTime": convert_time_format(arrival_info.get("actual", ""))
            }
        }
        old_api_format.append(old_api_flight)
    
    return old_api_format

# Read the new API data from a file
with open(input_file_path, 'r') as file:
    new_api_data = json.load(file)

# Process arrivals and departures
arrival_data = new_api_data.get("arrivals", [])
departure_data = new_api_data.get("departures", [])

# Extract the flight information from arrival_data
actual_arrival_data = arrival_data['data']  # This should be the list of flight dictionaries

# Similarly, do the same for departure_data if it has the same structure
actual_departure_data = departure_data['data']

# Now, pass these to your function
converted_arrivals = convert_to_old_api_format(actual_arrival_data, "arrival")
converted_departures = convert_to_old_api_format(actual_departure_data, "departure")


#converted_arrivals = convert_to_old_api_format(arrival_data, "arrival")
#converted_departures = convert_to_old_api_format(departure_data, "departure")

# Combine arrivals and departures
converted_data = converted_arrivals + converted_departures

# Check if conversion produced any results
if not converted_data:
    print("Conversion resulted in an empty list. Please check the conversion logic.")

# Write the converted data to a file
with open(output_file_path, 'w') as file:
    json.dump(converted_data, file, indent=4)

print(f"Converted data saved to {output_file_path}")

## From the combine.py fiile; combine the new daily data with combined_strip.json
import json

# Replace 'file1.json' and 'file2.json' with your actual file paths
file1_path = 'data\combined_strip.json'
file2_path = output_file_path

def read_json(file_path):
    try:
        with open(file_path, 'r') as file:
            return json.load(file)
    except Exception as e:
        print(f"Error reading {file_path}: {e}")
        return None

# Read the first file
data1 = read_json(file1_path)
if data1 is None or not isinstance(data1, list):
    print(f"File {file1_path} is empty or not a list")
else:
    print(f"File {file1_path} read successfully with {len(data1)} records")

# Read the second file
data2 = read_json(file2_path)
if data2 is None or not isinstance(data2, list):
    print(f"File {file2_path} is empty or not a list")
else:
    print(f"File {file2_path} read successfully with {len(data2)} records")

# Append and save only if both files are read successfully
if data1 is not None and data2 is not None:
    combined_data = data1 + data2
    output_path = 'data/combined_strip.json'
    with open(output_path, 'w') as file:
        json.dump(combined_data, file, indent=4)
    print(f"Combined data saved successfully with {len(combined_data)} records")
