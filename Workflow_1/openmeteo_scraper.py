import requests
import pandas as pd
import time

# Définissez ici votre fonction pour récupérer les données météorologiques
def fetch_weather_data():
    api_url = "https://archive-api.open-meteo.com/v1/archive"
    params = {
    "start_date": "2000-01-01",
    "end_date": "2023-10-28",
    "hourly": "temperature_2m,relativehumidity_2m,dewpoint_2m,apparent_temperature,precipitation,rain,snowfall,snow_depth,weathercode,pressure_msl,surface_pressure,cloudcover,cloudcover_low,cloudcover_mid,cloudcover_high,et0_fao_evapotranspiration,vapor_pressure_deficit,windspeed_10m,windspeed_100m,winddirection_10m,winddirection_100m,windgusts_10m,soil_temperature_0_to_7cm,soil_temperature_7_to_28cm,soil_temperature_28_to_100cm,soil_temperature_100_to_255cm,soil_moisture_0_to_7cm,soil_moisture_7_to_28cm,soil_moisture_28_to_100cm,soil_moisture_100_to_255cm,is_day,shortwave_radiation,direct_radiation,diffuse_radiation,direct_normal_irradiance,terrestrial_radiation,shortwave_radiation_instant,direct_radiation_instant,diffuse_radiation_instant,direct_normal_irradiance_instant,terrestrial_radiation_instant",
    "daily": "weathercode,temperature_2m_max,temperature_2m_min,temperature_2m_mean,apparent_temperature_max,apparent_temperature_min,apparent_temperature_mean,sunrise,sunset,precipitation_sum,rain_sum,snowfall_sum,precipitation_hours,windspeed_10m_max,windgusts_10m_max,winddirection_10m_dominant,shortwave_radiation_sum,et0_fao_evapotranspiration",
    "windspeed_unit": "ms",
    "timezone": "auto"
}


    response = requests.get(api_url, params=params)

    if response.status_code == 200:
        data = response.json()
        return data
    else:
        print(f"Failed to retrieve data. Status code: {response.status_code}")
        return None

# Fonction pour sauvegarder les données dans un fichier CSV
def save_to_csv(data, csv_file):
    if data is not None:
        # Créez des DataFrames à partir des données
        hourly_df = pd.DataFrame(data[0]['hourly'])
        daily_df = pd.DataFrame(data[0]['daily'])

        # Combinez les DataFrames
        df = pd.concat([hourly_df, daily_df], axis=1)

        # Enregistrez les données dans un fichier CSV
        df.to_csv(csv_file, index=False, encoding='utf-8')
        print(f"Data has been successfully saved to {csv_file}")

# Utilisez ces fonctions pour récupérer et sauvegarder les données
if __name__ == "__main__":
    csv_file = "openmeteo_data.csv"

    # Votre clé d'API et compteur de limites
    limit_remaining = 1000
    limit_reset = time.time() + 3600  # Exemple : une heure à partir de maintenant

    while limit_remaining > 0:
        current_time = time.time()
        if current_time >= limit_reset:
            # Votre limite d'accès a été réinitialisée, réinitialisez le compteur
            limit_remaining = 1000
            limit_reset = current_time + 3600  # Réinitialisez pour la prochaine heure

        # Vérifiez si vous avez des limites d'accès restantes
        if limit_remaining > 0:
            print(f"Remaining API limits: {limit_remaining}")
            data = fetch_weather_data()
            if data is not None:
                save_to_csv(data, csv_file)
                limit_remaining -= 1  # Réduisez le compteur
                # Vous pouvez également ajouter un délai ici si nécessaire pour respecter les limites d'accès
        else:
            print("No remaining API limits for this hour. Waiting for reset...")
            time.sleep(60)  # Attendez une minute avant de vérifier à nouveau

    print("API limits exhausted. Please wait for the next hour to continue.")
    
