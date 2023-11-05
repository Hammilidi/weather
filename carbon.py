import requests

url = "https://api.electricitymap.org/v3/carbon-intensity/latest?zone=MA"

response = requests.get(url)

if response.status_code == 200:
    data = response.json()
    print("Carbon Intensity Data for Morocco:", data)
else:
    print("Failed to retrieve carbon intensity data for Morocco. Status code:", response.status_code)
