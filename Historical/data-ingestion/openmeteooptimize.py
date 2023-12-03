import openmeteo_requests
import requests_cache
import pandas as pd
import logging
import os

# Configuration du logging
logging.basicConfig(filename='/home/FIL_ROUGE/Historical/Log/openmeteo.log', level=logging.INFO)

# Limite quotidienne d'appels
API_CALLS_LIMIT = 10000
api_calls_count = 0

def make_api_call():
    global api_calls_count
    # Votre code pour effectuer l'appel à l'API Open-Meteo
    # Assurez-vous d'incrémenter le compteur après chaque appel réussi
    api_calls_count += 1
    if api_calls_count > API_CALLS_LIMIT:
        logging.info('API Calls limit reached for the day.')
        raise Exception('API Calls limit reached for the day.')

try:
    # Initialisation de la session et de l'API Open-Meteo
    cache_session = requests_cache.CachedSession('/home/FIL_ROUGE/Historical/Caches/.openmeteo', expire_after=-1)
    openmeteo = openmeteo_requests.Client(session=cache_session)

    # Vos paramètres pour les appels à l'API Open-Meteo
    params = {
        "latitude": 34.9559,
        "longitude": -5.4437,
        "start_date": "2023-11-30",
        "end_date": "2023-12-01",
        "interval": 3600,  # Interval d'une heure pour les données horaires
        "variables": [
            "weather_code", "pressure_msl", "wind_speed_10m", "wind_speed_100m", 
            "wind_direction_10m", "wind_direction_100m", "wind_gusts_10m", 
            "soil_temperature_0_to_7cm", "soil_temperature_7_to_28cm", 
            "soil_moisture_0_to_7cm", "soil_moisture_7_to_28cm", 
            "shortwave_radiation", "direct_radiation", "diffuse_radiation", 
            "direct_normal_irradiance", "terrestrial_radiation",
            "temperature_2m_max", "temperature_2m_min", "temperature_2m_mean",
            "apparent_temperature_max", "apparent_temperature_min", "apparent_temperature_mean",
            "rain_sum", "snowfall_sum", "precipitation_hours",
            "shortwave_radiation_sum", "et0_fao_evapotranspiration"
        ],
        "timezone": "auto"
    }

    # Vérifier la limite des appels API avant chaque appel
    make_api_call()

    # Votre code pour récupérer les données de l'API
    url = "https://archive-api.open-meteo.com/v1/archive"
    responses = openmeteo.weather_api(url, params=params)

    # Traiter les données récupérées
    hourly_data = responses[0].Hourly().DataFrame()
    daily_data = responses[0].Daily().DataFrame()

    # Créer le répertoire s'il n'existe pas
    output_directory = "/home/FIL_ROUGE/Historical/data/raw/openmeteo"
    if not os.path.exists(output_directory):
        os.makedirs(output_directory)

    # Stocker les données dans des fichiers CSV
    hourly_data.to_csv(os.path.join(output_directory, "hourly_data.csv"), index=False)
    daily_data.to_csv(os.path.join(output_directory, "daily_data.csv"), index=False)

except Exception as e:
    logging.error(f"Error: {str(e)}")
    # Gérer l'erreur ou arrêter le script si la limite d'appels est atteinte
