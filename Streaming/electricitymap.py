import requests

def fetch_power_breakdown_data(zone="MA"):
    BaseURL = "https://api-access.electricitymaps.com/free-tier"
    endpoint = "/power-breakdown/latest"
    
    # Construct the full URL with the specified zone and endpoint
    url = f"{BaseURL}{endpoint}"
    
    # Replace "YOUR_API_KEY" with your actual API key
    api_key = "oQWn2xj45ukPWC3v1lMWJclIY43ngkKY"

    try:
        # Include the API key in the request headers
        headers = {'Authorization': f'Token {api_key}'}
        response = requests.get(url, params={'zone': zone}, headers=headers)
        
        if response.status_code == 200:
            data = response.json()
            return data
        else:
            print(f"Request failed with status code: {response.status_code}")
            print(response.text)  # Print the response content for additional information
            return None
    except requests.exceptions.RequestException as e:
        print(f"Error during the request: {e}")
        return None

# Example usage
power_breakdown_data = fetch_power_breakdown_data("MA")
if power_breakdown_data:
    print("Power breakdown data for zone MA:")
    print(power_breakdown_data)
