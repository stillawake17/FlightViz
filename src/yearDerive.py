import json
import pandas as pd
import plotly.express as px
import plotly.io as pio

# Load data
json_file_path = 'data\\combined_flights_data.json'
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

def process_and_visualize(year, df):
    df_year = df[df['Year'] == year].copy()  # Make a copy of the slice
    
    # Categorize by time
    df_year["Hour"] = df_year["actualTime"].dt.hour
    df_year["Minute"] = df_year["actualTime"].dt.minute
    df_year["Time_Category"] = "Regular arrivals"
    df_year.loc[(df_year["Hour"] == 23) & (df_year["Minute"] < 30), "Time_Category"] = "Shoulder hour flights"
    df_year.loc[(df_year["Hour"] == 23) & (df_year["Minute"] >= 30), "Time_Category"] = "Night hour arrivals"
    df_year.loc[(df_year["Hour"] < 6), "Time_Category"] = "Night hour arrivals"
    df_year.loc[(df_year["Hour"] == 6), "Time_Category"] = "Shoulder hour flights"


    # Counting flights
    total_flights = len(df_year)
    shoulder_hour_flights = len(df_year[df_year['Time_Category'] == 'Shoulder hour flights'])
    night_hour_flights = len(df_year[df_year['Time_Category'] == 'Night hour arrivals'])

    # Quotas, Categories, and Counts 
    quotas = [85990, 9500, 4000]
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
        title=f"Flight Counts as Percentage of Quotas by Category for {year}",
        text=[f"{percentage:.2f}%" for percentage in percentages]
    )
    fig.update_layout(showlegend=False)

    # Save the figure as an HTML div
    plot_div = pio.to_html(fig, full_html=False)
    with open(f"templates/plot_div_{year}.html", "w", encoding='utf-8') as f:
        f.write(plot_div)

# Process and visualize for each year
for year in [2023, 2024]:
    process_and_visualize(year, df)
