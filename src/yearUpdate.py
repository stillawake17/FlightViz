import json
import pandas as pd
import plotly.express as px
import plotly.io as pio

# Load data
json_file_path = 'data\\bristol_airport_2023-12-21_data.json'
with open(json_file_path, 'r') as file:
    data = json.load(file)

# Flatten and process the data
flattened_data = []
for entry in data:
    actualTime = None
    if 'arrival' in entry and 'actualTime' in entry['arrival'] and entry['arrival']['iataCode'] == 'brs':
        actualTime = entry['arrival']['actualTime']
    elif 'departure' in entry and 'actualTime' in entry['departure'] and entry['departure']['iataCode'] == 'brs':
        actualTime = entry['departure']['actualTime']

    if actualTime:
        flattened_data.append({
            'actualTime': actualTime
        })

df = pd.DataFrame(flattened_data)
df['actualTime'] = pd.to_datetime(df['actualTime'])

# Add a year column
df['Year'] = df['actualTime'].dt.year

# Ensure the Year column is added correctly
if 'Year' not in df.columns:
    raise ValueError("Year column not added. Please check the data.")

# Filtering the DataFrame for 2023 data
df_2023 = df[df['Year'] == 2023]

# 2024 data:
df_2024 = df[df['Year'] == 2024]

# 2025 data:
df_2025 = df[df['Year'] == 2025]

# Analysis for 2024 data can be done using df_2024
