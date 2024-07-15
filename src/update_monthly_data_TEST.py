from pymongo import MongoClient
from datetime import datetime, timedelta
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
            time_category = get_time_category(flight['arrival'].get('actual'))
            categorized_flights[time_category].append(flight)
        
        for flight in daily_data['departures']:
            time_category = get_time_category(flight['departure'].get('actual'))
            categorized_flights[time_category].append(flight)

    return categorized_flights

def get_time_category(dateTimeString):
    if not isinstance(dateTimeString, str):
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

def print_categorized_flights(start_date, end_date):
    flight_data = get_flight_data(start_date, end_date)
    for daily_data in flight_data:
        date = daily_data['date']
        categorized_flights = categorize_flights([daily_data])
        
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

def main():
    start_date = (datetime.now() - timedelta(days=4)).strftime("%Y-%m-%d")  # Modify this to set your start date
    end_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")    # Modify this to set your end date
    
    print_categorized_flights(start_date, end_date)

if __name__ == "__main__":
    main()
