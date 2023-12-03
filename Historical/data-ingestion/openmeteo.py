import openmeteo_requests
import requests_cache
import pandas as pd
from retry_requests import retry
import logging, os

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
    retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
    openmeteo = openmeteo_requests.Client(session=retry_session)

    # Vos paramètres pour les appels à l'API Open-Meteo
    params = {
        "latitude": 34.9559,
        "longitude": -5.4437,
        "start_date": "2023-11-30",
        "end_date": "2023-12-01",
        "hourly": ["weather_code", "pressure_msl", "wind_speed_10m", "wind_speed_100m", "wind_direction_10m", "wind_direction_100m", "wind_gusts_10m", "soil_temperature_0_to_7cm", "soil_temperature_7_to_28cm", "soil_temperature_28_to_100cm", "soil_temperature_100_to_255cm", "soil_moisture_0_to_7cm", "soil_moisture_7_to_28cm", "soil_moisture_28_to_100cm", "soil_moisture_100_to_255cm", "shortwave_radiation", "direct_radiation", "diffuse_radiation", "direct_normal_irradiance", "terrestrial_radiation"],
        "daily": ["weather_code", "temperature_2m_max", "temperature_2m_min", "temperature_2m_mean", "apparent_temperature_max", "apparent_temperature_min", "apparent_temperature_mean", "rain_sum", "snowfall_sum", "precipitation_hours", "shortwave_radiation_sum", "et0_fao_evapotranspiration"],
        "wind_speed_unit": "ms",
        "timezone": "auto"
    }

    # Boucle pour effectuer les appels et récupérer les données
    while True:
        try:
            make_api_call()  # Vérifie la limite des appels API avant chaque appel
            # Votre code pour récupérer les données de l'API
            url = "https://archive-api.open-meteo.com/v1/archive"
            responses = openmeteo.weather_api(url, params=params)

            # Traitement des données horaires
            response = responses[0]
            hourly = response.Hourly()

            # Création du dictionnaire de données horaires
            hourly_data = {
                "date": pd.date_range(
                    start=pd.to_datetime(hourly.Time(), unit="s"),
                    end=pd.to_datetime(hourly.TimeEnd(), unit="s"),
                    freq=pd.Timedelta(seconds=hourly.Interval()),
                    inclusive="left"
                ),
                    "weather_code": hourly.Variables(0).ValuesAsNumpy(),
                    "pressure_msl": hourly.Variables(1).ValuesAsNumpy(),
                    "wind_speed_10m": hourly.Variables(2).ValuesAsNumpy(),
                    "wind_speed_100m": hourly.Variables(3).ValuesAsNumpy(),
                    "wind_direction_10m": hourly.Variables(4).ValuesAsNumpy(),
                    "wind_direction_100m": hourly.Variables(5).ValuesAsNumpy(),
                    "wind_gusts_10m": hourly.Variables(6).ValuesAsNumpy(),
                    "soil_temperature_0_to_7cm": hourly.Variables(7).ValuesAsNumpy(),
                    "soil_temperature_7_to_28cm": hourly.Variables(8).ValuesAsNumpy(),
                    "soil_temperature_28_to_100cm": hourly.Variables(9).ValuesAsNumpy(),
                    "soil_temperature_100_to_255cm": hourly.Variables(10).ValuesAsNumpy(),
                    "soil_moisture_0_to_7cm": hourly.Variables(11).ValuesAsNumpy(),
                    "soil_moisture_7_to_28cm": hourly.Variables(12).ValuesAsNumpy(),
                    "soil_moisture_28_to_100cm": hourly.Variables(13).ValuesAsNumpy(),
                    "soil_moisture_100_to_255cm": hourly.Variables(14).ValuesAsNumpy(),
                    "shortwave_radiation": hourly.Variables(15).ValuesAsNumpy(),
                    "direct_radiation": hourly.Variables(16).ValuesAsNumpy(),
                    "diffuse_radiation": hourly.Variables(17).ValuesAsNumpy(),
                    "direct_normal_irradiance": hourly.Variables(18).ValuesAsNumpy(),
                    "terrestrial_radiation": hourly.Variables(19).ValuesAsNumpy(),
            }

            # Création du répertoire s'il n'existe pas
            output_directory = "/home/FIL_ROUGE/Historical/data/raw/openmeteo"
            if not os.path.exists(output_directory):
                os.makedirs(output_directory)
                
            # Création du DataFrame pour les données horaires
            hourly_dataframe = pd.DataFrame(data=hourly_data)

            # Stockage des données horaires dans un fichier CSV
            hourly_dataframe.to_csv(os.path.join(output_directory, "hourly_data.csv"), index=False)


            # Traitement des données journalières
            daily = response.Daily()

            # Création du dictionnaire de données journalières
            daily_data = {
                "date": pd.date_range(
                    start=pd.to_datetime(daily.Time(), unit="s"),
                    end=pd.to_datetime(daily.TimeEnd(), unit="s"),
                    freq=pd.Timedelta(seconds=daily.Interval()),
                    inclusive="left"
                ),
                    "weather_code": daily.Variables(0).ValuesAsNumpy(),
                    "temperature_2m_max": daily.Variables(1).ValuesAsNumpy(),
                    "temperature_2m_min": daily.Variables(2).ValuesAsNumpy(),
                    "temperature_2m_mean": daily.Variables(3).ValuesAsNumpy(),
                    "apparent_temperature_max": daily.Variables(4).ValuesAsNumpy(),
                    "apparent_temperature_min": daily.Variables(5).ValuesAsNumpy(),
                    "apparent_temperature_mean": daily.Variables(6).ValuesAsNumpy(),
                    "rain_sum": daily.Variables(7).ValuesAsNumpy(),
                    "snowfall_sum": daily.Variables(8).ValuesAsNumpy(),
                    "precipitation_hours": daily.Variables(9).ValuesAsNumpy(),
                    "shortwave_radiation_sum": daily.Variables(10).ValuesAsNumpy(),
                    "et0_fao_evapotranspiration": daily.Variables(11).ValuesAsNumpy(),
            }

            # Création du DataFrame pour les données journalières
            daily_dataframe = pd.DataFrame(data=daily_data)

            # Stockage des données journalières dans un fichier CSV
            daily_dataframe.to_csv(os.path.join(output_directory, "daily_data.csv"), index=False)

        except Exception as e:
            logging.error(f"Error: {str(e)}")
            # Gérer l'erreur ou arrêter le script si la limite d'appels est atteinte
            break

except Exception as ex:
    logging.error(f"Error during API call: {str(ex)}")
    # Gérer les erreurs générales
