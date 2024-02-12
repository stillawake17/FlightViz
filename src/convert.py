
#Run convert.py after app.py
import json
from datetime import datetime

# Path to the file containing the new API JSON response
input_file_path = 'EGGD_combined_2024-01-31.json'
# Path where the converted data will be saved
output_file_path = 'converted_data240131.json'

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
