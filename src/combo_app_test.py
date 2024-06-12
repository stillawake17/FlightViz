import requests
from config import api_key

def test_api():
    url = "http://api.aviationstack.com/v1/flights"
    params = {
        'access_key': api_key,
        'limit': 1
    }
    response = requests.get(url, params=params)
    if response.status_code == 200:
        print("API response:", response.json())
    else:
        print(f"Error fetching data: {response.status_code}")
        print(response.text)

test_api()

