import json
import pandas as pd
import os

def categorize_flight(arrival_time):
    hour = arrival_time.hour
    minute = arrival_time.minute
    if (hour == 23 and minute < 30) or hour == 6:
        return "Shoulder hour flights"
    if (hour == 23 and minute >= 30) or hour < 6:
        return "Night hour departures"
    return "Regular departures"

def process_arrival_data(file_path):
    with open(file_path, 'r') as file:
        flights_data = json.load(file)

    all_arrivals = []

    for flight in flights_data:
        if (flight['type'] == 'arrival' and 
            flight['status'] == 'landed' and
            'actualTime' in flight['arrival'] and
            flight['arrival']['iataCode'].lower() == 'brs'):
            actual_arrival_time = pd.to_datetime(flight['arrival'].get('actualTime', ''))
            flight_info = {
                'flight_number': flight['flight']['iataNumber'],
                'airline_name': flight['airline']['name'],
                'flight_status': flight['status'],
                'scheduled_arrival': flight['arrival']['scheduledTime'],
                'estimated_arrival': flight['arrival'].get('estimatedTime', ''),
                'actual_arrival': actual_arrival_time,
                'arrival_date': actual_arrival_time.date(),
                'arrival_time': actual_arrival_time.time(),
                'arrival_delay': flight['arrival'].get('delay', 0),
                'flight_type': categorize_flight(actual_arrival_time)
            }
            all_arrivals.append(flight_info)

    if not all_arrivals:
        print(f"No valid arrivals found in {file_path}.")
        return pd.DataFrame()

    df = pd.DataFrame(all_arrivals)
    return df

def append_arrivals_to_csv(arrivals_df, csv_file_path):
    if arrivals_df.empty:
        print("No data to append.")
        return {}

    header = not os.path.exists(csv_file_path)
    arrivals_df.to_csv(csv_file_path, mode='a', header=header, index=False)
    return {
        'count': len(arrivals_df),
        'average_actual_arrival': arrivals_df['actual_arrival'].mean()
    }

directory_path = './Aviation/BRS'
file_paths = [os.path.join(directory_path, file) for file in os.listdir(directory_path) if file.startswith('BRS_arrival_flights') and file.endswith('.json')]

base_csv_path = 'C:/Users/josti/OneDrive/Desktop/Gitlab clone/LDR/FlightViz/arrival_arrival_flights_summary'

arrival_stats = []

for file_path in file_paths:
    try:
        arrivals_df = process_arrival_data(file_path)
        if not arrivals_df.empty:
            date_str = file_path[-15:-5]  # Assuming the format 'BRS_arrival_flights_YYYY-MM-DD.json'
            csv_file_path = f'{base_csv_path}_{date_str}.csv'  # Create a specific path for each date
            day_summary = append_arrivals_to_csv(arrivals_df, csv_file_path)
            day_summary['date'] = date_str
            arrival_stats.append(day_summary)
            print(f"Processed and appended data from {file_path}")
        else:
            print(f"No valid arrivals found in {file_path}")
    except Exception as e:
        print(f"Failed to process {file_path}: {str(e)}")

if arrival_stats:
    summary_df = pd.DataFrame(arrival_stats)
    summary_df['average_actual_arrival'] = pd.to_datetime(summary_df['average_actual_arrival'])
    summary_df.fillna(0).to_csv('arrival_arrival_summary_statistics.csv', index=False)

print("All arrival flights data has been processed and summarized.")
