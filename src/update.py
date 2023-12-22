import requests

def get_flights(api_key, airport_iata):
    base_url = "https://aviation-edge.com/v2/public/flights"
    params = {
        'key': api_key,
        'arrIata': airport_iata,  # Use 'arrIata' for arrivals
    }
    response = requests.get(base_url, params=params)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error: {response.status_code}")
        return None

api_key = '97634b-cd2101'  # Replace with your API key
airport_iata = 'BRS'  # IATA code for Bristol Airport

# Get flight data
flights_data = get_flights(api_key, airport_iata)
if flights_data:
    # Process and use your data here
    print(flights_data)
else:
    print("No data retrieved.")
