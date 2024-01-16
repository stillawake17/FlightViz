import requests
import json
from datetime import datetime, timedelta
import os
from config import api_key 
import os
#api_key = os.getenv('api_key')

api_key = api_key


# Base URL for the API
base_url = "https://aviation-edge.com/v2/public/flightsHistory"
code = 'BRS' # 'BHX' 'BOL' 'CWL' 'LBA' 'LCY' 'LGW' 'SOU' 'NCL' 'LTN'

# Function to fetch data for a specific date range and type (arrival/departure)
def fetch_data(date_from, date_to, type):
    params = {
        'key': api_key,
        'code': code,
        'type': type,
        'date_from': date_from,
        'date_to': date_to
    }
    response = requests.get(base_url, params=params)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error fetching data from {date_from} to {date_to}: {response.status_code}")
        return None

# Define the path for the JSON file in the data directory
today_str = datetime.now().strftime("%Y-%m-%d")
json_file_path = f'data/{code}_airport_{today_str}_data.json'

# Check if the file exists and load existing data
if os.path.exists(json_file_path):
    with open(json_file_path, 'r') as file:
        all_data = json.load(file)
else:
    all_data = []

# Define the overall start and end dates for your data retrieval
overall_start_date = datetime(2024, 1, 3)  # Modify as needed
overall_end_date = datetime(2024, 1, 11)  # needs to be > 3 days from now

current_start_date = overall_start_date

# Loop through each 30-day period and fetch data
while current_start_date <= overall_end_date:
    current_end_date = min(current_start_date + timedelta(days=30), overall_end_date)
    start_date_str = current_start_date.strftime("%Y-%m-%d")
    end_date_str = current_end_date.strftime("%Y-%m-%d")
    
    print(f"Fetching data from {start_date_str} to {end_date_str}")

    # Fetch arrival data
    arrivals = fetch_data(start_date_str, end_date_str, 'arrival')
    if arrivals:
        all_data.extend(arrivals)

    # Fetch departure data
    departures = fetch_data(start_date_str, end_date_str, 'departure')
    if departures:
        all_data.extend(departures)

    # Move to the next period
    current_start_date = current_end_date + timedelta(days=1)

# Save combined data to the JSON file with today's date in the filename
with open(json_file_path, 'w') as file:
    json.dump(all_data, file)

print("Data collection complete.")
