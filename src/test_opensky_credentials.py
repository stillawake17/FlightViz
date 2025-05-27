#!/usr/bin/env python3
"""
Enhanced test script to verify OpenSky credentials with multiple auth methods
"""

import json
import requests
import os
import base64

def test_credentials():
    print("=== OpenSky Credentials Test ===\n")
    
    # Check if credentials file exists
    if not os.path.exists("credentials.json"):
        print("❌ credentials.json file not found!")
        return False
    
    # Load credentials
    try:
        with open("credentials.json", 'r') as f:
            creds = json.load(f)
        print("✅ credentials.json file loaded successfully")
        print(f"📄 File contents: {creds}")
    except Exception as e:
        print(f"❌ Error loading credentials: {e}")
        return False
    
    # Extract credentials (handle different formats)
    username = (creds.get('username') or 
               creds.get('clientId') or 
               creds.get('client_id'))
    password = (creds.get('client_secret') or 
               creds.get('clientSecret') or 
               creds.get('password'))
    
    if not username or not password:
        print("❌ Could not find valid credentials in file")
        print("Expected fields: clientId + clientSecret OR username + client_secret")
        return False
    
    print(f"🔑 Using username: {username}")
    print(f"🔑 Password length: {len(password)} characters")
    
    # Try different authentication methods
    methods = [
        ("HTTP Basic Auth (requests.auth)", test_basic_auth),
        ("Manual Authorization Header", test_manual_auth),
        ("Anonymous (no auth)", test_anonymous)
    ]
    
    success = False
    for method_name, test_func in methods:
        print(f"\n🧪 Testing {method_name}...")
        if test_func(username, password):
            print(f"✅ {method_name} successful!")
            success = True
            break
        else:
            print(f"❌ {method_name} failed")
    
    return success

def test_basic_auth(username, password):
    """Test using requests built-in basic auth"""
    try:
        session = requests.Session()
        session.auth = (username, password)
        
        url = "https://opensky-network.org/api/states/all"
        params = {'icao24': 'test'}
        
        response = session.get(url, params=params, timeout=15)
        
        print(f"📊 Response status: {response.status_code}")
        if response.status_code == 401:
            print("🔍 401 Unauthorized - credentials rejected")
            return False
        elif response.status_code == 200:
            print("✅ Authentication successful!")
            check_rate_limits(response)
            return True
        else:
            print(f"⚠️  Unexpected status: {response.status_code}")
            return False
            
    except Exception as e:
        print(f"❌ Error: {e}")
        return False

def test_manual_auth(username, password):
    """Test using manual Authorization header"""
    try:
        # Create base64 encoded credentials
        credentials = f"{username}:{password}"
        encoded_credentials = base64.b64encode(credentials.encode()).decode()
        
        headers = {
            'Authorization': f'Basic {encoded_credentials}',
            'User-Agent': 'OpenSky-Python-Client/1.0'
        }
        
        url = "https://opensky-network.org/api/states/all"
        params = {'icao24': 'test'}
        
        response = requests.get(url, params=params, headers=headers, timeout=15)
        
        print(f"📊 Response status: {response.status_code}")
        if response.status_code == 401:
            print("🔍 401 Unauthorized - credentials rejected")
            return False
        elif response.status_code == 200:
            print("✅ Authentication successful!")
            check_rate_limits(response)
            return True
        else:
            print(f"⚠️  Unexpected status: {response.status_code}")
            return False
            
    except Exception as e:
        print(f"❌ Error: {e}")
        return False

def test_anonymous(username, password):
    """Test anonymous access (no authentication)"""
    try:
        url = "https://opensky-network.org/api/states/all"
        params = {'icao24': 'test'}
        
        response = requests.get(url, params=params, timeout=15)
        
        print(f"📊 Response status: {response.status_code}")
        if response.status_code == 200:
            print("✅ Anonymous access works!")
            print("⚠️  Note: You'll have lower rate limits (400 vs 4000 requests/day)")
            check_rate_limits(response)
            return True
        else:
            print(f"❌ Anonymous access failed: {response.status_code}")
            return False
            
    except Exception as e:
        print(f"❌ Error: {e}")
        return False

def check_rate_limits(response):
    """Check and display rate limit information"""
    remaining = response.headers.get('X-Rate-Limit-Remaining')
    reset = response.headers.get('X-Rate-Limit-Reset')
    
    if remaining:
        print(f"📈 API calls remaining: {remaining}")
    if reset:
        print(f"📅 Rate limit resets: {reset}")
    
    # Try to parse response
    try:
        data = response.json()
        if isinstance(data, dict) and 'states' in data:
            state_count = len(data['states']) if data['states'] else 0
            print(f"📦 Current aircraft states available: {state_count}")
        else:
            print("📦 Response received but in unexpected format")
    except:
        print("📦 Response is not valid JSON")

def test_bristol_specific():
    """Test a Bristol-specific request"""
    print("\n=== Testing Bristol-Specific Request ===")
    
    try:
        # Try anonymous first since auth might be having issues
        bristol_lat = 51.3827
        bristol_lon = -2.7191
        lat_offset = 0.05  # Smaller area to reduce load
        lon_offset = 0.07
        
        params = {
            'lamin': bristol_lat - lat_offset,
            'lomin': bristol_lon - lon_offset,
            'lamax': bristol_lat + lat_offset,
            'lomax': bristol_lon + lon_offset
        }
        
        print("🛩️  Requesting aircraft around Bristol Airport (anonymous)...")
        response = requests.get("https://opensky-network.org/api/states/all", 
                               params=params, timeout=15)
        
        print(f"📊 Response status: {response.status_code}")
        
        if response.status_code == 200:
            data = response.json()
            if data and data.get('states'):
                aircraft_count = len(data['states'])
                print(f"✅ Found {aircraft_count} aircraft near Bristol!")
                
                # Show first few aircraft
                for i, state in enumerate(data['states'][:3]):
                    icao24 = state[0] or 'Unknown'
                    callsign = (state[1] or '').strip() or 'Unknown'
                    country = state[2] or 'Unknown'
                    lat = state[6]
                    lon = state[5]
                    altitude = state[7]
                    on_ground = state[8]
                    
                    status = "🛬 On Ground" if on_ground else f"✈️ {altitude}m"
                    print(f"   {i+1}. {callsign} ({icao24}) - {country} - {status}")
                
                return True
            else:
                print("ℹ️  No aircraft currently near Bristol (normal outside peak hours)")
                return True
        else:
            print(f"❌ Bristol request failed: {response.status_code}")
            if response.status_code == 429:
                print("⏰ Rate limit exceeded - try again later")
            return False
            
    except Exception as e:
        print(f"❌ Error testing Bristol area: {e}")
        return False

def check_opensky_status():
    """Check if OpenSky API is generally available"""
    print("\n=== Checking OpenSky API Status ===")
    
    try:
        # Simple ping to check if API is up
        response = requests.get("https://opensky-network.org/api/states/all?icao24=test", timeout=10)
        
        if response.status_code in [200, 401]:
            print("✅ OpenSky API is responding")
            return True
        elif response.status_code == 503:
            print("⚠️  OpenSky API is temporarily unavailable (503)")
            return False
        else:
            print(f"⚠️  OpenSky API returned status: {response.status_code}")
            return False
            
    except requests.exceptions.Timeout:
        print("⏰ OpenSky API is not responding (timeout)")
        return False
    except requests.exceptions.ConnectionError:
        print("🌐 Cannot connect to OpenSky API")
        return False
    except Exception as e:
        print(f"❌ Error checking API status: {e}")
        return False

if __name__ == "__main__":
    # First check if API is available at all
    if not check_opensky_status():
        print("\n❌ OpenSky API appears to be unavailable. Try again later.")
        exit(1)
    
    # Test credentials
    success = test_credentials()
    
    # Test Bristol area regardless of auth success
    test_bristol_specific()
    
    if success:
        print("\n🎉 Authentication is working!")
        print("🚀 You can now run the main OpenSky collector script.")
    else:
        print("\n🔧 Authentication failed, but you can still use anonymous access.")
        print("📊 Anonymous limits: 400 requests/day")
        print("🔍 Consider checking your OpenSky account settings or contacting support.")
        
    print("\n💡 Next steps:")
    print("1. Try running the main collector with anonymous access")
    print("2. Compare results with your AviationStack data")
    print("3. Contact OpenSky support if authentication continues to fail")