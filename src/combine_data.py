import json

def load_json_data(file_path):
    with open(file_path, 'r') as file:
        data = json.load(file)
        # Ensure the data is a list
        if isinstance(data, dict):
            # If the data is a dictionary, extract values assuming they are lists
            data = list(data.values())
        elif not isinstance(data, list):
            raise ValueError("Data format is not supported. Expected a list or a dictionary.")
        return data

# Load your existing data
combined_data = load_json_data('data/combined_flights_data.json')

# Load the new data
new_data = load_json_data('data/BRS_airport_2024-01-05_data.json')

# Combine the datasets
combined_data.extend(new_data)

# Remove duplicates
unique_data = []
seen = set()

for entry in combined_data:
    # Ensure entry is a dictionary
    if not isinstance(entry, dict):
        continue

    # Define a unique identifier for each entry
    identifier = (entry['flight']['number'], 
                  entry['departure']['scheduledTime'], 
                  entry['arrival']['scheduledTime'])

    if identifier not in seen:
        seen.add(identifier)
        unique_data.append(entry)

# Save the deduplicated data back to a JSON file
with open('merged_flights_data.json', 'w') as file:
    json.dump(unique_data, file, indent=4)

print(f"Merged data contains {len(unique_data)} entries.")

# Save the deduplicated data to a JSON file
output_file_path = 'data/combined_flights_data2.json'  # Replace with your desired file path

with open(output_file_path, 'w') as file:
    json.dump(unique_data, file, indent=4)

print("Data has been successfully saved to", output_file_path)
