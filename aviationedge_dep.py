import json
import pandas as pd
import os
import re

def categorize_flight(departure_time):
    """ Categorize flights based on the time of departure. """
    hour = departure_time.hour
    minute = departure_time.minute
    if (hour == 23 and minute < 30) or hour == 6:
        return "Shoulder hour flights"
    if (hour == 23 and minute >= 30) or hour < 6:
        return "Night hour departures"
    return "Regular departures"

def process_departure_data(file_path):
    """ Load departure data from a JSON file and process it into a structured DataFrame. """
    with open(file_path, 'r') as file:
        flights_data = json.load(file)

    all_departures = []

    for flight in flights_data:
        if (flight['type'] == 'departure' and 
            flight['status'] == 'active' and
            'actualTime' in flight['departure'] and
            flight['departure']['iataCode'].lower() == 'brs'):
            actual_departure_time = pd.to_datetime(flight['departure'].get('actualTime', ''))
            flight_info = {
                'flight_number': flight['flight']['iataNumber'],
                'airline_name': flight['airline']['name'],
                'flight_status': flight['status'],
                'scheduled_departure': flight['departure']['scheduledTime'],
                'estimated_departure': flight['departure'].get('estimatedTime', ''),
                'actual_departure': actual_departure_time,
                'departure_date': actual_departure_time.date(),
                'departure_time': actual_departure_time.time(),
                'departure_delay': flight['departure'].get('delay', 0),
                'flight_type': categorize_flight(actual_departure_time)
            }
            all_departures.append(flight_info)

    if not all_departures:
        print(f"No valid departures found in {file_path}.")
        return pd.DataFrame()

    return pd.DataFrame(all_departures)

def append_departures_to_csv(departures_df, csv_file_path):
    """ Append departures data to a CSV file. """
    if departures_df.empty:
        print("No data to append.")
        return

    header = not os.path.exists(csv_file_path)
    departures_df.to_csv(csv_file_path, mode='a', header=header, index=False)
    print(f"Appended data to {csv_file_path}")

def extract_date_from_filename(file_path):
    """ Extract the date from the filename using regular expressions. """
    match = re.search(r'\d{4}-\d{2}-\d{2}', file_path)
    if match:
        return match.group(0)
    else:
        raise ValueError("No valid date found in filename")

def save_summary_statistics(departures_df, date_str, summary_csv_path):
    """ Save summary statistics to a CSV file. """
    summary_stats = departures_df.groupby(['departure_date', 'flight_type']).agg(
        total_flights=('flight_number', 'count'),
        avg_departure_delay=('departure_delay', 'mean')
    ).reset_index()

    header = not os.path.exists(summary_csv_path)
    summary_stats.to_csv(summary_csv_path, mode='a', header=header, index=False)
    print(f"Saved summary statistics to {summary_csv_path}")

# Directory where the files are stored
directory_path = './Aviation/BRS'
file_paths = [os.path.join(directory_path, file) for file in os.listdir(directory_path) if file.startswith('BRS_departure_flights') and file.endswith('.json')]

base_csv_path = 'C:/Users/josti/OneDrive/Desktop/Gitlab clone/LDR/FlightViz/avi_departure_flights_summary.csv'
summary_csv_path = 'C:/Users/josti/OneDrive/Desktop/Gitlab clone/LDR/FlightViz/avi_departure_flights_summary_statistics.csv'

for file_path in file_paths:
    try:
        date_str = extract_date_from_filename(file_path)
        dated_csv_path = f"{os.path.splitext(base_csv_path)[0]}_{date_str}.csv"
        departures_df = process_departure_data(file_path)
        if not departures_df.empty:
            append_departures_to_csv(departures_df, dated_csv_path)
            save_summary_statistics(departures_df, date_str, summary_csv_path)
            print(f"Processed and appended data from {file_path}")
        else:
            print(f"No valid departures found in {file_path}")
    except Exception as e:
        print(f"Failed to process {file_path}: {str(e)}")

print("All departure flights data has been processed and summarized.")
