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

# Categorize by time
df["Hour"] = df["actualTime"].dt.hour
df["Minute"] = df["actualTime"].dt.minute
df["Time_Category"] = "Regular arrivals"
df.loc[(df["Hour"] == 23) & (df["Minute"] < 30), "Time_Category"] = "Shoulder hour flights"
df.loc[(df["Hour"] == 23) & (df["Minute"] >= 30), "Time_Category"] = "Night hour arrivals"
df.loc[(df["Hour"] < 6), "Time_Category"] = "Night hour arrivals"
df.loc[(df["Hour"] == 6), "Time_Category"] = "Shoulder hour flights"

# Counting flights
total_flights = len(df)
shoulder_hour_flights = len(df[df['Time_Category'] == 'Shoulder hour flights'])
night_hour_flights = len(df[df['Time_Category'] == 'Night hour arrivals'])

# Quotas
quotas = [85990, 9500, 4000]  # Update these values if needed

# Categories and counts
categories = ['Total Flights', 'Shoulder Hour Flights', 'Night Hour Flights']
counts = [total_flights, shoulder_hour_flights, night_hour_flights]

# Calculating percentages
percentages = [(count/quota)*100 for count, quota in zip(counts, quotas)]

# Creating a Plotly figure
fig = px.bar(
    x=percentages,
    y=categories,
    color=categories,
    orientation='h',
    title="Flight Counts as Percentage of Quotas by Category",
    text=[f"{percentage:.2f}%" for percentage in percentages]
)

# Optionally, you can adjust the layout of the plot
fig.update_layout(showlegend=False)

# Save the figure as an HTML div
plot_div = pio.to_html(fig, full_html=False)
with open("templates/plot_div.html", "w", encoding='utf-8') as f:
    f.write(plot_div)
