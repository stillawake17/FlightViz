import requests
import json
import os
import time
from datetime import datetime, timedelta
import config  # Your existing config.py file with API_TOKEN
import random
import sys

# Constants
BRISTOL_AIRPORT_CODE = 'BRS'  # Bristol Airport ICAO code
BASE_URL = 'https://fr24api.flightradar24.com/api'  # Updated base URL
OUTPUT_DIR = 'flight_data'  # Directory to store JSON files
MAX_RETRIES = 3  # Maximum number of retry attempts for rate-limited requests
RETRY_DELAY = 60  # Base delay in seconds between retries (will be randomized)

# Create output directory if it doesn't exist
if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)

# Create a subdirectory for historical data
HISTORICAL_DIR = os.path.join(OUTPUT_DIR, 'historical')
if not os.path.exists(HISTORICAL_DIR):
    os.makedirs(HISTORICAL_DIR)

# Create authentication headers
def create_auth_headers():
    return {
        'Content-Type': 'application/json',
        'Accept': 'application/json',
        'Authorization': f'Bearer {config.FR24_API_KEY}',
        'Accept-Version': 'v1'  # This header is required
    }

def fetch_with_rate_limit(url, headers, max_retries=MAX_RETRIES):
    """Make a request with rate limit handling and legal restriction checks"""
    for attempt in range(max_retries):
        try:
            print(f"Making request to {url}")
            response = requests.get(url, headers=headers)
            
            # If the request is successful, return the response
            if response.status_code == 200:
                return response
                
            # If we hit a rate limit (429)
            elif response.status_code == 429:
                wait_time = RETRY_DELAY + random.randint(5, 20)  # Add jitter to avoid thundering herd
                
                # Check if there's a Retry-After header and use that if available
                if 'Retry-After' in response.headers:
                    wait_time = int(response.headers['Retry-After'])
                
                print(f"Rate limited. Waiting {wait_time} seconds before retry {attempt + 1}/{max_retries}")
                time.sleep(wait_time)
                
                # Continue to the next retry attempt
                continue
            
            # If we hit a legal restriction (451)
            elif response.status_code == 451:
                print(f"ERROR: Data unavailable for legal reasons (451 status code)")
                print(f"The requested data at {url} cannot be provided due to legal restrictions")
                print(f"This usually indicates the data is blocked by FlightRadar24 due to licensing or regulatory requirements")
                raise LegalRestrictionError(url)
                
            # Credit issue (402)
            elif response.status_code == 402:
                print(f"ERROR: Insufficient credits (402 status code)")
                print(f"Your account doesn't have enough credits to perform this request")
                raise InsufficientCreditsError(url)
                
            # For other errors, raise an exception
            else:
                response.raise_for_status()
                
        except requests.exceptions.RequestException as e:
            # Check if this is a legal restriction error
            if hasattr(e, 'response') and e.response is not None and e.response.status_code == 451:
                print(f"ERROR: Data unavailable for legal reasons (451 status code)")
                print(f"The requested data at {url} cannot be provided due to legal restrictions")
                raise LegalRestrictionError(url)
            
            # For network errors, retry with exponential backoff
            if attempt < max_retries - 1:  # Don't sleep after the last attempt
                wait_time = RETRY_DELAY * (2 ** attempt) + random.randint(1, 10)
                print(f"Request error: {e}. Retrying in {wait_time} seconds. Attempt {attempt + 1}/{max_retries}")
                time.sleep(wait_time)
            else:
                # If we've exhausted all retries, raise the exception
                raise e
                
    # If we've exhausted all retries and still haven't returned, raise an exception
    raise Exception(f"Failed to get response after {max_retries} attempts")

# Custom exceptions for API errors
class LegalRestrictionError(Exception):
    """Raised when data is unavailable due to legal restrictions"""
    def __init__(self, url):
        self.url = url
        self.message = f"Data at {url} is unavailable due to legal restrictions (451 error)"
        super().__init__(self.message)

class InsufficientCreditsError(Exception):
    """Raised when account has insufficient credits"""
    def __init__(self, url):
        self.url = url
        self.message = f"Request to {url} failed due to insufficient credits (402 error)"
        super().__init__(self.message)

def fetch_airport_info():
    """Fetch basic information about Bristol Airport"""
    try:
        print("Fetching airport information...")
        response = fetch_with_rate_limit(
            f'{BASE_URL}/static/airports/{BRISTOL_AIRPORT_CODE}/light',
            create_auth_headers()
        )
        data = response.json()
        
        # Save to file
        with open(os.path.join(OUTPUT_DIR, 'airport_info.json'), 'w') as f:
            json.dump(data, f, indent=2)
            
        print("Airport information saved successfully.")
        return data
    except LegalRestrictionError as e:
        print(f"Legal restriction encountered: {e.message}")
        return None
    except Exception as e:
        print(f"Error fetching airport information: {e}")
        return None
    
def fetch_historical_day(date, limit=None):
    """Fetch historical flight data for a specific day with rate limiting"""
    date_str = date.strftime("%Y-%m-%d")
    filename = f"{date_str}.json"
    file_path = os.path.join(HISTORICAL_DIR, filename)
    
    # Skip if we already have this day's data
    if os.path.exists(file_path):
        print(f"Data for {date_str} already exists, skipping...")
        return True
    
    try:
        print(f"Fetching historical data for {date_str}...")
        
        # Format date for FlightRadar API (YYYY-MM-DD)
        formatted_date = date.strftime("%Y-%m-%d")
        
        # Make the API call for historical flights with rate limiting
        url = f'{BASE_URL}/historic/flight-positions/full'
# Get Unix timestamp for midnight on the specified date
        unix_timestamp = int(datetime.combine(date, datetime.min.time()).timestamp())
        params = {
                    'airports': f'both:{BRISTOL_AIRPORT_CODE}',  # specify direction
                    'timestamp': unix_timestamp
                }
        
        # Add limit parameter if provided to control credit usage
        if limit:
            params['limit'] = limit
        
        try:
            response = fetch_with_rate_limit(
                url, 
                create_auth_headers(), 
                params=params
            )
            data = response.json()
        except LegalRestrictionError:
            # Create a placeholder file with legal restriction information
            data = {
                'metadata': {
                    'airport_code': BRISTOL_AIRPORT_CODE,
                    'date': date_str,
                    'fetched_at': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    'legal_restriction': True,
                    'error': "This data is unavailable due to legal restrictions (451 error)"
                },
                'flights': []  # Empty flights list
            }
            print(f"Creating placeholder file for {date_str} due to legal restrictions")
        except InsufficientCreditsError:
            print(f"Insufficient credits to fetch data for {date_str}")
            return False
        
        # If we got here normally, add metadata
        if 'metadata' not in data:
            data['metadata'] = {
                'airport_code': BRISTOL_AIRPORT_CODE,
                'date': date_str,
                'fetched_at': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                'legal_restriction': False
            }
        
        # Save to file named with the date
        with open(file_path, 'w') as f:
            json.dump(data, f, indent=2)
            
        print(f"Historical data for {date_str} saved successfully.")
        
        # Add a delay between requests to avoid rate limiting
        delay = random.randint(2, 5)  # Random delay between 2-5 seconds
        print(f"Waiting {delay} seconds before next request...")
        time.sleep(delay)
        
        return True
    except Exception as e:
        print(f"Error fetching historical data for {date_str}: {e}")
        return False

def fetch_usage_data():
    """Fetch account usage data to monitor credit consumption"""
    try:
        print("Fetching usage data...")
        response = fetch_with_rate_limit(
            f'{BASE_URL}/usage',
            create_auth_headers()
        )
        data = response.json()
        
        # Save to file
        with open(os.path.join(OUTPUT_DIR, 'usage_data.json'), 'w') as f:
            json.dump(data, f, indent=2)
            
        print("Usage data saved successfully.")
        print("Recent credit usage:")
        for item in data.get('data', []):
            print(f"- Endpoint: {item.get('endpoint')}, Requests: {item.get('request_count')}, Credits: {item.get('credits')}")
        
        return data
    except Exception as e:
        print(f"Error fetching usage data: {e}")
        return None

def create_historical_index():
    """Create an index file listing all available historical data files"""
    try:
        print("Creating historical data index...")
        
        # List all JSON files in the historical directory
        files = [f for f in os.listdir(HISTORICAL_DIR) if f.endswith('.json')]
        files.sort(reverse=True)  # Sort by date, newest first
        
        # Extract dates from filenames
        dates = []
        legal_restricted_dates = []
        
        for file in files:
            date_str = file.replace('.json', '')
            file_path = os.path.join(HISTORICAL_DIR, file)
            
            try:
                # Validate it's a proper date
                date_obj = datetime.strptime(date_str, "%Y-%m-%d")
                
                # Check if this is a legal restriction placeholder
                is_legal_restricted = False
                try:
                    with open(file_path, 'r') as f:
                        data = json.load(f)
                        if 'metadata' in data and data['metadata'].get('legal_restriction') is True:
                            is_legal_restricted = True
                except:
                    pass  # If we can't read the file, assume it's not legally restricted
                
                date_info = {
                    'date': date_str,
                    'filename': file,
                    'display_date': date_obj.strftime("%B %d, %Y"),  # More readable format
                    'legal_restriction': is_legal_restricted
                }
                
                if is_legal_restricted:
                    legal_restricted_dates.append(date_info)
                else:
                    dates.append(date_info)
                    
            except ValueError:
                # Skip files that don't have valid date names
                continue
        
        # Create the index
        index = {
            'last_updated': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            'available_dates': dates,
            'legal_restricted_dates': legal_restricted_dates,
            'airport_code': BRISTOL_AIRPORT_CODE
        }
        
        # Save the index
        with open(os.path.join(OUTPUT_DIR, 'historical_index.json'), 'w') as f:
            json.dump(index, f, indent=2)
            
        print(f"Historical index created with {len(dates)} available dates and {len(legal_restricted_dates)} legally restricted dates.")
        return True
    except Exception as e:
        print(f"Error creating historical index: {e}")
        return False

def fetch_date_range(start_date, end_date, limit=None):
    """Fetch historical data for a specific date range with optional limit to control credit usage"""
    print(f"Starting historical data fetch from {start_date} to {end_date}...")
    
    # Create a list of dates from start_date to end_date
    date_list = []
    current_date = start_date
    while current_date <= end_date:
        date_list.append(current_date)
        current_date += timedelta(days=1)
    
    # Fetch data for each date
    success_count = 0
    legal_restriction_count = 0
    credit_issue_count = 0
    total_count = len(date_list)
    
    print(f"Will attempt to fetch data for {total_count} days from {start_date} to {end_date}")
    if limit:
        print(f"Using limit={limit} to control credit usage")
    
    # Check usage before starting
    print("Checking current credit usage before starting...")
    fetch_usage_data()
    
    for i, date in enumerate(date_list):
        print(f"Processing day {i+1}/{total_count}: {date}")
        try:
            if fetch_historical_day(date, limit):
                success_count += 1
        except LegalRestrictionError:
            legal_restriction_count += 1
        except InsufficientCreditsError:
            credit_issue_count += 1
            print("Stopping due to insufficient credits")
            break
    
    # Create the index file
    create_historical_index()
    
    # Check usage after completion
    print("Checking credit usage after completion...")
    fetch_usage_data()
    
    print(f"Historical data fetch completed:")
    print(f"- Successfully fetched: {success_count} days")
    print(f"- Legal restrictions: {legal_restriction_count} days")
    print(f"- Credit issues: {credit_issue_count} days")
    print(f"- Failed: {total_count - success_count - legal_restriction_count - credit_issue_count} days")

def fetch_last_month():
    """Fetch historical data for the past two months with rate limiting"""
    print("Starting historical data fetch for the last two months...")
    
    # Get yesterday's date (instead of today)
    yesterday = datetime.now().date() - timedelta(days=1)
    
    # Calculate date from two months ago
    two_months_ago = yesterday - timedelta(days=30)
    
    # Fetch the date range
    fetch_date_range(two_months_ago, yesterday)  # End with yesterday instead of today - 1

def fetch_last_week():
    """Fetch historical data for just the past week (useful for testing or limited API quotas)"""
    print("Starting historical data fetch for the last week...")
    
    # Get yesterday's date
    yesterday = datetime.now().date() - timedelta(days=1)
    
    # Calculate date from one week ago
    one_week_ago = yesterday - timedelta(days=7)
    
    # Fetch the date range
    fetch_date_range(one_week_ago, yesterday)  # End with yesterday

def fetch_single_day(date_str, limit=None):
    """Fetch historical data for a single specified day with optional limit to control credit usage"""
    try:
        date = datetime.strptime(date_str, "%Y-%m-%d").date()
        print(f"Fetching data for single day: {date}")
        
        if fetch_historical_day(date, limit):
            print(f"Successfully fetched data for {date}")
        else:
            print(f"Failed to fetch data for {date}")
            
        # Update the index
        create_historical_index()
    except ValueError:
        print(f"Invalid date format: {date_str}. Please use YYYY-MM-DD format.")

def run_full_update(limit=None):
    """Run a full update of airport info and historical data with optional limit to control credit usage"""
    print(f"Starting full data update for Bristol Airport ({BRISTOL_AIRPORT_CODE}) at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Fetch basic airport information
    fetch_airport_info()
    
    # Check current usage
    fetch_usage_data()
    
    # Fetch historical data for the past two months
    fetch_last_month(limit)
    
    print("All data fetching completed.")

def print_usage():
    """Print usage instructions"""
    print("\nFlightRadar24 Historical Data Fetcher")
    print("====================================")
    print("\nUsage:")
    print("  python fetch_historical_flight_data.py [command] [options]")
    print("\nCommands:")
    print("  full       - Fetch full two months of data (default)")
    print("  week       - Only fetch the last week of data")
    print("  index      - Just update the index without fetching new data")
    print("  day YYYY-MM-DD - Fetch data for a specific day")
    print("  range YYYY-MM-DD YYYY-MM-DD - Fetch data for a specific date range")
    print("  credits    - Check your current credit usage")
    print("\nOptions:")
    print("  --limit N  - Limit the number of flights returned per request to control credit usage")
    print("\nExamples:")
    print("  python fetch_historical_flight_data.py full")
    print("  python fetch_historical_flight_data.py full --limit 100")
    print("  python fetch_historical_flight_data.py day 2025-03-01 --limit 50")
    print("  python fetch_historical_flight_data.py range 2025-02-15 2025-03-01")
    print("  python fetch_historical_flight_data.py credits")

if __name__ == "__main__":
    # Process command line arguments
    if len(sys.argv) > 1:
        cmd = sys.argv[1].lower()
        
        # Check for limit option
        limit = None
        for i, arg in enumerate(sys.argv):
            if arg == "--limit" and i+1 < len(sys.argv):
                try:
                    limit = int(sys.argv[i+1])
                    print(f"Using limit of {limit} flights per request to control credit usage")
                except ValueError:
                    print(f"Invalid limit value: {sys.argv[i+1]}. Using no limit.")
        
        if cmd == "full":
            run_full_update(limit)
        elif cmd == "week":
            fetch_last_week(limit)
        elif cmd == "index":
            create_historical_index()
        elif cmd == "credits":
            fetch_usage_data()
        elif cmd == "day" and len(sys.argv) > 2:
            fetch_single_day(sys.argv[2], limit)
        elif cmd == "range" and len(sys.argv) > 3:
            try:
                start_date = datetime.strptime(sys.argv[2], "%Y-%m-%d").date()
                end_date = datetime.strptime(sys.argv[3], "%Y-%m-%d").date()
                fetch_date_range(start_date, end_date, limit)
            except ValueError:
                print("Invalid date format. Please use YYYY-MM-DD format.")
                print_usage()
        else:
            print_usage()
    else:
        # Default behavior - run full update
        run_full_update()