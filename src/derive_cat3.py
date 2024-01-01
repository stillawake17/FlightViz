import json
import pandas as pd
import calendar

# Load data
json_file_path = 'data\\combined_flights_data2.json'
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
        flattened_data.append({'actualTime': actualTime})

df = pd.DataFrame(flattened_data)
df['actualTime'] = pd.to_datetime(df['actualTime'])

# Categorize by time
df["Hour"] = df["actualTime"].dt.hour
df["Minute"] = df["actualTime"].dt.minute
df["Time_Category"] = "Regular arrivals"
df.loc[(df["Hour"] == 23) & (df["Minute"] < 30), "Time_Category"] = "Shoulder hour flights"
df.loc[(df["Hour"] == 23) & (df["Minute"] >= 30), "Time_Category"] = "Night hour arrivals"
df.loc[(df["Hour"] < 6), "Time_Category"] = "Night hour arrivals"
df.loc[(df["Hour"] == 6), "Time_Category"] = "Shoulder hour flights"

# Extract and convert month
df["Month"] = df["actualTime"].dt.month
df["Month_Name"] = df["Month"].apply(lambda x: calendar.month_name[x])

# Group by month and count categories
monthly_counts = df.groupby(["Month_Name", "Time_Category"]).size().reset_index(name='Counts')

print(monthly_counts)
