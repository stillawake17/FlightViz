import pandas as pd
from datetime import datetime
import json

# Define the path to the CSV file
file_path = 'data\\flightbyflight_12.csv'

# Read the data, trying different encoding if necessary
try:
    data = pd.read_csv(file_path)
except UnicodeDecodeError:
    data = pd.read_csv(file_path, encoding='iso-8859-1')

# Define a function to convert date and time into ISO 8601 format
def convert_to_iso8601(row):
    try:
        date_str = f"{row['Date']} {row['ATD']}"
        date_obj = datetime.strptime(date_str, '%d-%b-%y %H:%M')
        return date_obj.isoformat(timespec='milliseconds')
    except ValueError:
        # This will return None for rows with invalid date/time data
        return None

# Apply the conversion function to each row
data['actualTime'] = data.apply(convert_to_iso8601, axis=1)

# Filter out rows with invalid date/time data
data = data.dropna(subset=['actualTime'])

# Determine if Bristol is the departure or arrival airport
def get_airport_codes(row, airport_field):
    if row[airport_field] == 'Bristol (BRS)':
        return {"iataCode": "BRS", "icaoCode": "EGGD"}
    else:
        # Replace with actual IATA and ICAO codes of the other airport
        return {"iataCode": "other_airport_iata", "icaoCode": "other_airport_icao"}

# Create a new JSON structure
json_data = data.apply(lambda row: {
    "type": "arrival" if row['To'] == 'Bristol (BRS)' else "departure",
    "status": row['Status'].lower(),
    "departure": get_airport_codes(row, 'From'),
    "arrival": get_airport_codes(row, 'To'),
    "actualTime": row['actualTime']
}, axis=1).tolist()

# Convert to JSON format
json_formatted_str = json.dumps(json_data, indent=4)

# Output the JSON to a file
output_file = 'output_data.json'
with open(output_file, 'w') as f:
    f.write(json_formatted_str)

# Print out the path to the new JSON file
print(f"JSON file created at: {output_file}")
