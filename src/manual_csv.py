import pandas as pd
import numpy as np

# Path to your CSV file
file_path = 'data/flightbyflight.csv'

# Reload your dataset with the specified encoding
data = pd.read_csv(file_path, encoding='ISO-8859-1')

# Convert 'Date' column to datetime format
data['Date'] = pd.to_datetime(data['Date'], format='%d-%b-%y')

# Handle 'ATD' conversion to datetime.time, managing errors by coercing them
data['ATD'] = pd.to_datetime(data['ATD'], format='%H:%M', errors='coerce').dt.time

# Combine 'Date' and 'ATD' into 'DateTime', handling potential NaT in 'ATD'
data['DateTime'] = pd.to_datetime(data['Date'].astype(str) + ' ' + data['ATD'].astype(str), errors='coerce')
data['DateTime'] = data['DateTime'].dt.strftime('%Y-%m-%dT%H:%M:%S.000')

# Extract time from 'Status' and create 'Landed' datetime
data['ExtractedTime'] = data['Status'].str.extract('(\d{2}:\d{2})')
data['ExtractedTime'] = pd.to_datetime(data['ExtractedTime'], format='%H:%M', errors='coerce').dt.time
data['Landed'] = pd.to_datetime(data['Date'].astype(str) + ' ' + data['ExtractedTime'].astype(str), errors='coerce')
data['Landed'] = data['Landed'].dt.strftime('%Y-%m-%dT%H:%M:%S.000')


# Check for 'Bristol' in 'From' and 'To' columns and set 'actualTime' accordingly
# Using str.contains with case=False to ignore case sensitivity and na=False to handle missing values
data['actualTime'] = np.where(data['From'].str.contains("Bristol", case=False, na=False), data['DateTime'],
                              np.where(data['To'].str.contains("Bristol", case=False, na=False), data['Landed'], pd.NaT))

# Dropping the 'Unnamed: 11' column if it exists
if 'Unnamed: 11' in data.columns:
    data = data.drop(columns=['Unnamed: 11'])

# Display the DataFrame to verify the changes
print(data[['From', 'To', 'DateTime', 'Landed', 'actualTime']].head())

# Define the path where you want to save your CSV file
output_file_path = 'data/processed_manual_data.csv'

# Export the DataFrame to a CSV file
data.to_csv(output_file_path, index=False, encoding='utf-8')

# If you don't want to include the DataFrame index, ensure `index=False` is set as shown above.


