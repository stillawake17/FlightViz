import json

def load_json_data(file_path):
    """
    Load JSON data from a specified file path.
    """
    with open(file_path, 'r') as file:
        return json.load(file)

def filter_flight_data_safe(flight_data):
    """
    Filter the necessary fields from a flight data entry.
    """
    # Create a new dictionary to store the filtered data
    filtered_data = {
        'type': flight_data.get('type', None),
        'status': flight_data.get('status', None),
        'departure': {
            'iataCode': flight_data.get('departure', {}).get('iataCode', None),
            'icaoCode': flight_data.get('departure', {}).get('icaoCode', None),
            'actualTime': flight_data.get('departure', {}).get('actualTime', None)
        },
        'arrival': {
            'iataCode': flight_data.get('arrival', {}).get('iataCode', None),
            'icaoCode': flight_data.get('arrival', {}).get('icaoCode', None),
            'actualTime': flight_data.get('arrival', {}).get('actualTime', None)
        }
    }

    return filtered_data

# Example usage of the script

# Path to your JSON file
file_path = 'data\\combined_flights_data2.json'  

# Load the data from the file
loaded_data = load_json_data(file_path)

# Process each entry in the loaded data safely
filtered_data_safe = [filter_flight_data_safe(entry) for entry in loaded_data]

# Save the filtered data to a new file
filtered_file_path_safe = 'data\\combined_flights_data_strip.json'  
with open(filtered_file_path_safe, 'w') as file:
    json.dump(filtered_data_safe, file, indent=4)


