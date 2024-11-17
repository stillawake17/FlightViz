import requests
import json
from datetime import datetime, timedelta
from config import api_key  # Assuming your API key is stored in a separate config file

base_url = "http://api.aviationstack.com/v1/flights"
code = 'EGGD'  # Your airport code

def fetch_data(date, is_arrival=True, filename=None):
    all_data = []  # Initialize an empty list to hold all records
    params = {
        'access_key': api_key,
        'flight_date': date,
        'limit': 100,  # Adjust limit based on API documentation
        'offset': 0  # Initially start at 0
    }

    if is_arrival:
        params['arr_icao'] = code  # For flights arriving at the specified code
    else:
        params['dep_icao'] = code  # For flights departing from the specified code

    while True:
        response = requests.get(base_url, params=params)
        if response.status_code == 200:
            data = response.json()
            all_data.extend(data.get('data', []))  # Append the fetched records to all_data list
            
            # Check if we fetched fewer records than the limit, indicating we are done
            if len(data.get('data', [])) < params['limit']:
                break
            
            params['offset'] += params['limit']  # Increment the offset to fetch the next batch
        else:
            print(f"Error fetching data for {date}: {response.status_code}")
            print(response.text)
            break  # Exit the loop in case of an error

    if filename:
        with open(filename, 'w') as file:
            json.dump(all_data, file)  # Save all fetched data to the specified file
        print(f"Data saved to {filename}")

    return all_data  # Return the combined data for further processing


# Get yesterday's date by deleting one day in days=1
yesterday = (datetime.now() - timedelta(days=2)).strftime("%Y-%m-%d")

# Fetch and save arrival data
arrival_filename = f'EGGD_arrivals_{yesterday}.json'
fetch_data(yesterday, is_arrival=True, filename=arrival_filename)

# Fetch and save departure data
departure_filename = f'EGGD_departures_{yesterday}.json'
fetch_data(yesterday, is_arrival=False, filename=departure_filename)

# Combine data for processing
combined_filename = f'EGGD_combined_{yesterday}.json'

try:
    # Read arrival data
    with open(arrival_filename, 'r') as file:
        arrivals = json.load(file)

    # Read departure data
    with open(departure_filename, 'r') as file:
        departures = json.load(file)

    # Debug print to verify data count
    print(f"Arrivals count: {len(arrivals)}")
    print(f"Departures count: {len(departures)}")

    # Combine the data
    combined_data = {
        'arrivals': arrivals,
        'departures': departures
    }

    # Write the combined data to a new file
    with open(combined_filename, 'w') as file:
        json.dump(combined_data, file, indent=4)

    print(f"Combined data saved to {combined_filename}")

except FileNotFoundError as e:
    print(f"Error: {e}")

# Function to convert time formats
def convert_time_format(time_str):
    if time_str:
        # Parse the datetime from the new format
        dt = datetime.fromisoformat(time_str)
        # Format it to the old format (without timezone information)
        return dt.strftime("%Y-%m-%dt%H:%M:%S.000")
    return ""

# Function to convert to the old API format, adjusting for codeshare issues
def convert_to_old_api_format(flight_data, flight_type):
    old_api_format = []
    processed_flights = set()  # Track flight numbers to avoid duplicates

    for flight in flight_data:
        # Check if the codeshared field exists and skip if the primary flight has been processed
        codeshared = flight['flight'].get('codeshared')
        if codeshared:
            primary_number = codeshared.get('flight_number')
            if primary_number in processed_flights:
                continue  # Skip this as we already counted the primary flight

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

        # Add primary flight number to the processed list
        flight_number = flight["flight"]["number"]
        processed_flights.add(flight_number)

        old_api_format.append(old_api_flight)

    return old_api_format

# Convert data to old API format and save to a new JSON file
input_file_path = f'EGGD_combined_{yesterday}.json'
output_file_path = f'converted_{yesterday}.json'

# Read the combined data
with open(input_file_path, 'r') as file:
    new_api_data = json.load(file)

# Process arrivals and departures
arrival_data = new_api_data.get("arrivals", [])
departure_data = new_api_data.get("departures", [])

# Convert the data to the old API format
converted_arrivals = convert_to_old_api_format(arrival_data, "arrival")
converted_departures = convert_to_old_api_format(departure_data, "departure")

# Combine arrivals and departures
converted_data = converted_arrivals + converted_departures

# Check if conversion produced any results
if not converted_data:
    print("Conversion resulted in an empty list. Please check the conversion logic.")

# Write the converted data to a file
with open(output_file_path, 'w') as file:
    json.dump(converted_data, file, indent=4)

print(f"Converted data saved to {output_file_path}")

# Combine new data with existing data
file1_path = 'data/combined_strip.json'
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
