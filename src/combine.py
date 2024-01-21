import json

file_names = [
    'data/converted_data.json',
    'data/combined_flights_data_strip.json'
]

# Initialize a dictionary to hold combined data
combined_data = {'arrivals': [], 'departures': []}

for file_name in file_names:
    try:
        with open(file_name, 'r') as file:
            print(f"Reading {file_name}...")
            data = json.load(file)

            # Combine arrivals data
            if 'arrivals' in data and 'data' in data['arrivals']:
                combined_data['arrivals'].extend(data['arrivals']['data'])
                print(f"Added {len(data['arrivals']['data'])} arrival records from {file_name}")
            else:
                print(f"'arrivals' or 'data' key not found in {file_name}")

            # Combine departures data
            if 'departures' in data and 'data' in data['departures']:
                combined_data['departures'].extend(data['departures']['data'])
                print(f"Added {len(data['departures']['data'])} departure records from {file_name}")
            else:
                print(f"'departures' or 'data' key not found in {file_name}")

    except FileNotFoundError as e:
        print(f"Error: File {file_name} not found. {e}")
    except json.JSONDecodeError as e:
        print(f"Error: Could not parse JSON in {file_name}. {e}")

# Print final combined data length
print(f"Total arrival records in combined data: {len(combined_data['arrivals'])}")
print(f"Total departure records in combined data: {len(combined_data['departures'])}")

# Save the combined data to a new file
combined_filename = 'combined_flights_strip.json'
with open(combined_filename, 'w') as file:
    json.dump(combined_data, file)

print(f"Combined data saved to {combined_filename}")
