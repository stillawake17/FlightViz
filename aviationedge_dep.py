

import json
import pandas as pd

def process_flight_data(file_path):
    # Load JSON data from the file
    with open(file_path, 'r') as file:
        flights_data = json.load(file)

    # Filter for flights with actual departure time and create a DataFrame
    flights_with_actual_time = [flight for flight in flights_data if 'actualTime' in flight['departure'] and flight['status'] == 'active']
    df = pd.DataFrame([{
        'flight_number': flight['flight']['iataNumber'],
        'airline_name': flight['airline']['name'],
        'flight_status': flight['status'],
        'scheduled_departure': flight['departure']['scheduledTime'],
        'estimated_departure': flight['departure'].get('estimatedTime', ''),
        'actual_departure': flight['departure'].get('actualTime', ''),
        'departure_delay': flight['departure'].get('delay', 0)
    } for flight in flights_with_actual_time])

    # Convert time strings to datetime objects
    df['actual_departure'] = pd.to_datetime(df['actual_departure'])

    # Categorize flights by actual departure time
    def categorize_flight(row):
        hour = row['actual_departure'].hour
        minute = row['actual_departure'].minute
        if (hour == 23 and minute < 30) or hour == 6:
            return "Shoulder hour flights"
        if (hour == 23 and minute >= 30) or hour < 6:
            return "Night hour departures"
        return "Regular departures"

    df['time_category'] = df.apply(categorize_flight, axis=1)
    
    # Filter for night and shoulder hour flights
    special_flights_df = df[df['time_category'].isin(['Night hour departures', 'Shoulder hour flights'])]
    
    return df, special_flights_df

def append_flights_to_csv(flights_df, special_flights_df, csv_file_path, special_csv_file_path):
    # General flights data appending
    header = not pd.io.common.file_exists(csv_file_path)
    flights_df.to_csv(csv_file_path, mode='a', header=header, index=False)

    # Special flights data appending
    special_header = not pd.io.common.file_exists(special_csv_file_path)
    special_flights_df.to_csv(special_csv_file_path, mode='a', header=special_header, index=False)

    # Return counts for summary
    return special_flights_df['time_category'].value_counts().to_dict()

# List of file paths
file_paths = ['BRS_departure_flights_2023-05-01.json', 
              'BRS_departure_flights_2023-05-02.json', 
              'BRS_departure_flights_2023-05-03.json',
              'BRS_departure_flights_2023-05-04.json',
              'BRS_departure_flights_2023-05-05.json',
              'BRS_departure_flights_2023-05-06.json',
              'BRS_departure_flights_2023-05-07.json',
              'BRS_departure_flights_2023-05-08.json',
              'BRS_departure_flights_2023-05-09.json',
              'BRS_departure_flights_2023-05-10.json',
              'BRS_departure_flights_2023-05-11.json',
              'BRS_departure_flights_2023-05-12.json',
              'BRS_departure_flights_2023-05-13.json',
              'BRS_departure_flights_2023-05-14.json',
              'BRS_departure_flights_2023-05-15.json',
              'BRS_departure_flights_2023-05-16.json',
              'BRS_departure_flights_2023-05-17.json',
              'BRS_departure_flights_2023-05-18.json',
              'BRS_departure_flights_2023-05-19.json',
              'BRS_departure_flights_2023-05-20.json',
              'BRS_departure_flights_2023-05-21.json',
              'BRS_departure_flights_2023-05-22.json',
              'BRS_departure_flights_2023-05-23.json',
              'BRS_departure_flights_2023-05-24.json',
              'BRS_departure_flights_2023-05-25.json',
              'BRS_departure_flights_2023-05-26.json',
              'BRS_departure_flights_2023-05-27.json',
              'BRS_departure_flights_2023-05-28.json',
              'BRS_departure_flights_2023-05-29.json',
              'BRS_departure_flights_2023-05-30.json',
              'BRS_departure_flights_2023-05-31.json'
]

summary_csv_path = 'departure_flights_summary.csv'
special_summary_csv_path = 'special_flights_summary.csv'

summary_stats = []

for file_path in file_paths:
    try:
        full_flights_df, special_flights_df = process_flight_data(file_path)
        day_summary = append_flights_to_csv(full_flights_df, special_flights_df, summary_csv_path, special_summary_csv_path)
        day_summary['date'] = file_path[-14:-5]  # Extracting date from the file name
        summary_stats.append(day_summary)
        print(f"Processed and appended data from {file_path}")
    except Exception as e:
        print(f"Failed to process {file_path}: {str(e)}")

# Save the summary statistics to a separate CSV
if summary_stats:
    pd.DataFrame(summary_stats).fillna(0).to_csv('night_shoulder_summary_statistics.csv', index=False)

print("All departure flights data has been processed and summarized.")
