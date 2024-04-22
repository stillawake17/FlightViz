import pandas as pd
import os

# Directory containing the CSV files
directory = r'C:\Users\josti\OneDrive\Desktop\Gitlab clone\LDR\FlightViz'

# List to hold data from each CSV
dataframes = []

# Loop through the range of dates
for i in range(6, 20):  # Adjust the range according to your files
    date_str = f'202404{i:02}'  # Formats i as two digits
    file_path = os.path.join(directory, f'flight_data_{date_str}.csv')
    if os.path.exists(file_path):
        df = pd.read_csv(file_path)
        dataframes.append(df)
    else:
        print(f"No file found for date: {date_str}")

# Concatenate all dataframes
combined_df = pd.concat(dataframes, ignore_index=True)

# Write the combined dataframe to a new CSV file
combined_csv_path = os.path.join(directory, 'combined_airline_data.csv')
combined_df.to_csv(combined_csv_path, index=False)

print("Combined CSV file created:", combined_csv_path)
