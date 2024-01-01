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
df["Time_Category"] = "Regular flights"
df.loc[(df["Hour"] == 23) & (df["Minute"] < 30), "Time_Category"] = "Shoulder hour flights"
df.loc[(df["Hour"] == 23) & (df["Minute"] >= 30), "Time_Category"] = "Night hour flights"
df.loc[(df["Hour"] < 6), "Time_Category"] = "Night hour flights"
df.loc[(df["Hour"] == 6), "Time_Category"] = "Shoulder hour flights"

# Total counts for each category
total_flights = len(df)
shoulder_hour_flights = len(df[df['Time_Category'] == 'Shoulder hour flights'])
night_hour_flights = len(df[df['Time_Category'] == 'Night hour flights'])

# Quotas
quotas = [85990, 9500, 4000]  # Total Flights, Shoulder Hour Flights, Night Hour Flights quotas

# Categories and counts
categories = ['Total Flights', 'Shoulder Hour Flights', 'Night Hour Flights']
counts = [total_flights, shoulder_hour_flights, night_hour_flights]

# Calculating percentages
percentages = [(count/quota)*100 for count, quota in zip(counts, quotas)]

# Print categories, counts, and percentages
for category, count, percentage in zip(categories, counts, percentages):
    print(f"{category}: Count = {count}, Percentage = {percentage:.2f}% of Quota")
