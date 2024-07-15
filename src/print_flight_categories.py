from pymongo import MongoClient
from datetime import datetime, timedelta
import json
from config import mongo_uri

# Connect to MongoDB
client = MongoClient(mongo_uri)
db = client['flight_data']
collection = db['daily_flights']

def get_flight_data(start_date, end_date):
    query = {
        'date': {
            '$gte': start_date,
            '$lte': end_date
        }
    }
    return list(collection.find(query))

def categorize_flights(flight_data):
    categorized_flights = {
        'night': [],
        'shoulder': [],
        'regular': [],
        'unknown': []
    }
    
    for daily_data in flight_data:
        for flight in daily_data['arrivals']:
            if flight['flight_status'] == 'landed':
                actual_time = flight['arrival'].get('actual')
                if actual_time:
                    time_category = get_time_category(actual_time)
                    categorized_flights[time_category].append(flight)
                else:
                    categorized_flights['unknown'].append(flight)
        
        for flight in daily_data['departures']:
            if flight['flight_status'] == 'landed':
                actual_time = flight['departure'].get('actual')
                if actual_time:
                    time_category = get_time_category(actual_time)
                    categorized_flights[time_category].append(flight)
                else:
                    categorized_flights['unknown'].append(flight)

    return categorized_flights

def get_time_category(dateTimeString):
    if dateTimeString is None or not isinstance(dateTimeString, str):
        return "unknown"

    try:
        date = datetime.fromisoformat(dateTimeString)
    except ValueError:
        return "unknown"

    hour = date.hour
    minute = date.minute

    if (hour == 23 and minute >= 30) or hour < 6:
        return "night"
    if (hour == 23 and minute < 30) or hour == 6:
        return "shoulder"
    return "regular"

def summarize_daily_flights(categorized_flights):
    daily_summary = {
        'night': len(categorized_flights['night']),
        'shoulder': len(categorized_flights['shoulder']),
        'regular': len(categorized_flights['regular']),
        'unknown': len(categorized_flights['unknown']),
        'total': sum(len(flights) for flights in categorized_flights.values())
    }
    return daily_summary

def summarize_monthly_flights(daily_summaries):
    monthly_summary = {
        'night': sum(day['night'] for day in daily_summaries),
        'shoulder': sum(day['shoulder'] for day in daily_summaries),
        'regular': sum(day['regular'] for day in daily_summaries),
        'unknown': sum(day['unknown'] for day in daily_summaries),
        'total': sum(day['total'] for day in daily_summaries)
    }
    return monthly_summary

def print_categorized_flights(start_date, end_date):
    flight_data = get_flight_data(start_date, end_date)
    daily_summaries = []

    for daily_data in flight_data:
        date = daily_data['date']
        categorized_flights = categorize_flights([daily_data])
        daily_summary = summarize_daily_flights(categorized_flights)
        daily_summaries.append(daily_summary)

        print(f"Flights for {date}:")
        
        print("Night Flights:")
        for flight in categorized_flights['night']:
            print(f"  Flight Number: {flight['flight']['number']}, Actual Time: {flight['arrival'].get('actual', flight['departure'].get('actual', 'Unknown'))}")
        
        print("Shoulder Flights:")
        for flight in categorized_flights['shoulder']:
            print(f"  Flight Number: {flight['flight']['number']}, Actual Time: {flight['arrival'].get('actual', flight['departure'].get('actual', 'Unknown'))}")
        
        print("Regular Flights:")
        for flight in categorized_flights['regular']:
            print(f"  Flight Number: {flight['flight']['number']}, Actual Time: {flight['arrival'].get('actual', flight['departure'].get('actual', 'Unknown'))}")
        
        print("Unknown Category Flights:")
        for flight in categorized_flights['unknown']:
            print(f"  Flight Number: {flight['flight']['number']}, Actual Time: {flight['arrival'].get('actual', flight['departure'].get('actual', 'Unknown'))}")
        
        print("Daily Summary:")
        print(json.dumps(daily_summary, indent=4))
        print()

    monthly_summary = summarize_monthly_flights(daily_summaries)

    print("Monthly Summary:")
    print(json.dumps(monthly_summary, indent=4))

def main():
    start_date = (datetime.now() - timedelta(days=14)).strftime("%Y-%m-%d")  # Modify this to set your start date
    end_date = (datetime.now() - timedelta(days=2)).strftime("%Y-%m-%d")    # Modify this to set your end date
    
    print_categorized_flights(start_date, end_date)

if __name__ == "__main__":
    main()
