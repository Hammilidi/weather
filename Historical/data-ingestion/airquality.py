import openmeteo_requests
import os
import requests_cache
import pandas as pd
from retry_requests import retry
import logging

# Configuration du logging
logging.basicConfig(filename='/home/FIL_ROUGE/Historical/Log/air.log', level=logging.INFO)

# Setup the Open-Meteo API client with cache and retry on error
cache_session = requests_cache.CachedSession('/home/FIL_ROUGE/Historical/Caches/.air', expire_after=3600)
retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
openmeteo = openmeteo_requests.Client(session=retry_session)

try:
    # Assurez-vous que toutes les variables météorologiques requises sont listées ici
    # L'ordre des variables dans les données horaires est important pour les attribuer correctement ci-dessous
    url = "https://air-quality-api.open-meteo.com/v1/air-quality"
    params = {
        "latitude": 34.9559,
        "longitude": -5.4437,
        "hourly": ["pm10", "pm2_5", "carbon_monoxide", "nitrogen_dioxide", "sulphur_dioxide", "ozone"],
        "timezone": "auto",
        "start_date": "2022-07-29",
        "end_date": "2023-11-05"
    }
    responses = openmeteo.weather_api(url, params=params)

    # Process first location. Add a for-loop for multiple locations or weather models
    response = responses[0]
    print(f"Coordinates {response.Latitude()}°E {response.Longitude()}°N")
    print(f"Elevation {response.Elevation()} m asl")
    print(f"Timezone {response.Timezone()} {response.TimezoneAbbreviation()}")
    print(f"Timezone difference to GMT+0 {response.UtcOffsetSeconds()} s")

    # Process hourly data. The order of variables needs to be the same as requested.
    hourly = response.Hourly()
    hourly_pm10 = hourly.Variables(0).ValuesAsNumpy()
    hourly_pm2_5 = hourly.Variables(1).ValuesAsNumpy()
    hourly_carbon_monoxide = hourly.Variables(2).ValuesAsNumpy()
    hourly_nitrogen_dioxide = hourly.Variables(3).ValuesAsNumpy()
    hourly_sulphur_dioxide = hourly.Variables(4).ValuesAsNumpy()
    hourly_ozone = hourly.Variables(5).ValuesAsNumpy()

    hourly_data = {"date": pd.date_range(
        start=pd.to_datetime(hourly.Time(), unit="s"),
        end=pd.to_datetime(hourly.TimeEnd(), unit="s"),
        freq=pd.Timedelta(seconds=hourly.Interval()),
        inclusive="left"
    )}
    hourly_data["pm10"] = hourly_pm10
    hourly_data["pm2_5"] = hourly_pm2_5
    hourly_data["carbon_monoxide"] = hourly_carbon_monoxide
    hourly_data["nitrogen_dioxide"] = hourly_nitrogen_dioxide
    hourly_data["sulphur_dioxide"] = hourly_sulphur_dioxide
    hourly_data["ozone"] = hourly_ozone

    hourly_dataframe = pd.DataFrame(data=hourly_data)
    print(hourly_dataframe)

    # Création du répertoire s'il n'existe pas
    output_directory = "/home/FIL_ROUGE/Historical/data/raw/air"
    if not os.path.exists(output_directory):
        os.makedirs(output_directory)

    # Remplacez 'tanger_weather_data.csv' par le nom de fichier de sortie souhaité
    csv_file = os.path.join(output_directory, "tanger_air.csv")

    # Enregistrez les données dans un fichier CSV
    hourly_dataframe.to_csv(csv_file, index=False, encoding='utf-8')
    print(f"Data has been successfully saved to {csv_file}")

except Exception as e:
    logging.error(f"Error: {str(e)}")
