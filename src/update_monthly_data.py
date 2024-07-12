from pymongo import MongoClient
from datetime import datetime, timedelta
import json
import subprocess
from config import mongo_uri

# Connect to MongoDB
client = MongoClient(mongo_uri)
db = client['flight_data']
collection = db['daily_flights']

monthly_file_path = 'monthly_data.json'
combined_file_path = 'combined_output2.json'

def read_json(file_path):
    try:
        with open(file_path, 'r') as file:
            return json.load(file)
    except FileNotFoundError:
        print(f"File {file_path} not found. Creating a new one.")
        return None
    except Exception as e:
        print(f"Error reading {file_path}: {e}")
        return None

def save_json(data, file_path):
    try:
        with open(file_path, 'w') as file:
            json.dump(data, file, indent=4)
        print(f"Data saved to {file_path}")
    except Exception as e:
        print(f"Error saving data to {file_path}: {e}")

def get_flight_data(start_date, end_date):
    query = {
        'date': {
            '$gte': start_date,
            '$lte': end_date
        }
    }
    return list(collection.find(query))

def summarize_flight_data(flight_data):
    summary = {'total': 0, 'shoulder': 0, 'night': 0}
    
    for daily_data in flight_data:
        for flight in daily_data['arrivals'] + daily_data['departures']:
            if flight['flight_status'] == 'landed':
                summary['total'] += 1
                time_category = get_time_category(flight['arrival']['actual'] if 'arrival' in flight else flight['departure']['actual'])
                if time_category == 'Shoulder hour flights':
                    summary['shoulder'] += 1
                elif time_category == 'Night hour arrivals':
                    summary['night'] += 1

    return summary

def get_time_category(dateTimeString):
    date = datetime.fromisoformat(dateTimeString)
    hour = date.hour
    minute = date.minute

    if hour == 23 and minute < 30:
        return "Shoulder hour flights"
    if (hour == 23 and minute >= 30) or hour < 6:
        return "Night hour arrivals"
    if hour == 6:
        return "Shoulder hour flights"
    return "Regular arrivals"

def update_monthly_data(monthly_data, daily_summary, year, month):
    if str(year) not in monthly_data:
        monthly_data[str(year)] = {"total": [], "shoulder": [], "night": []}

    for key in ["total", "shoulder", "night"]:
        while len(monthly_data[str(year)][key]) < month:
            monthly_data[str(year)][key].append(0)

    monthly_data[str(year)]["total"][month-1] += daily_summary["total"]
    monthly_data[str(year)]["shoulder"][month-1] += daily_summary["shoulder"]
    monthly_data[str(year)]["night"][month-1] += daily_summary["night"]

    return monthly_data

def update_combined_data(combined_data, daily_summary, date):
    year, month, _ = date.split('-')
    month = int(month)

    if str(year) not in combined_data:
        combined_data[str(year)] = {"total": [], "shoulder": [], "night": []}

    for key in ["total", "shoulder", "night"]:
        while len(combined_data[str(year)][key]) < month:
            combined_data[str(year)][key].append(0)

    combined_data[str(year)]["total"][month-1] += daily_summary["total"]
    combined_data[str(year)]["shoulder"][month-1] += daily_summary["shoulder"]
    combined_data[str(year)]["night"][month-1] += daily_summary["night"]

    return combined_data

def main():
    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    flight_data = get_flight_data(yesterday, yesterday)
    daily_summary = summarize_flight_data(flight_data)
    
    monthly_data = read_json(monthly_file_path)
    if monthly_data is None:
        monthly_data = {}

    today = datetime.now()
    year = today.year
    month = today.month
    monthly_data = update_monthly_data(monthly_data, daily_summary, year, month)

    save_json(monthly_data, monthly_file_path)

    combined_data = read_json(combined_file_path)
    if combined_data is None:
        combined_data = {}

    combined_data = update_combined_data(combined_data, daily_summary, yesterday)
    save_json(combined_data, combined_file_path)

    # Push the updated file to Git
    subprocess.run(['bash', 'push_to_git.sh'])

if __name__ == "__main__":
    main()
