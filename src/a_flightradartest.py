import requests

from config import FR24_API_KEY
# Replace with your actual API token
API_TOKEN = FR24_API_KEY

# Define headers
headers = {
    'Accept': 'application/json',
    'Accept-Version': 'v1',
    'Authorization': f'Bearer {API_TOKEN}'
}

# Make the request
response = requests.get(
    'https://fr24api.flightradar24.com/api/static/airports/BRS/light',
    headers=headers
)

# Check and print the response
if response.status_code == 200:
    print(response.json())
else:
    print(f"Error: {response.status_code}")
    print(response.text)