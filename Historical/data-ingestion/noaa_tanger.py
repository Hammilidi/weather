import requests
import os
import gzip
from bs4 import BeautifulSoup
import logging

# Configuration du logging
logging.basicConfig(filename='/home/FIL_ROUGE/Historical/Log/noaa_tanger.log', level=logging.INFO)

# Spécifiez l'USAF de la station que vous souhaitez collecter.
usaf_station = "601010"  # Exemple : Station de Tanger

# Créez l'URL de la station en utilisant le code USAF
BASE_URL = "https://www1.ncdc.noaa.gov/pub/data/noaa"

# Année de début
start_year = 1980
end_year = 2024

# Créez un répertoire pour stocker les fichiers téléchargés
download_directory = "/home/FIL_ROUGE/Historical/raw/noaa_tanger"
os.makedirs(download_directory, exist_ok=True)

try:
    # Obtenez le contenu HTML de la page principale
    response = requests.get(BASE_URL)
    if response.status_code == 200:
        soup = BeautifulSoup(response.text, 'html.parser')

        for year_url in soup.find_all('a'):
            year = year_url.get('href')
            station_year_url = f"{BASE_URL}/{year}"

            # Obtenez le contenu HTML de l'année
            response_year = requests.get(station_year_url)
            if response_year.status_code == 200:
                soup_year = BeautifulSoup(response_year.text, 'html.parser')
                for file_url in soup_year.find_all('a'):
                    file_name = file_url.get('href')
                    if file_name is not None and file_name.endswith(".gz") and usaf_station in file_name:
                        # Téléchargez le fichier
                        file_path = os.path.join(download_directory, file_name)
                        file_url = f"{station_year_url}/{file_name}"
                        response = requests.get(file_url)
                        if response.status_code == 200:
                            with open(file_path, "wb") as file:
                                file.write(response.content)

                            # Décompressez le fichier
                            with gzip.open(file_path, 'rb') as f_in:
                                with open(file_path[:-3], 'wb') as f_out:
                                    f_out.writelines(f_in)

                            print(f"Décompressé : {file_name}")
                            # Supprimez le fichier compressé si nécessaire
                            os.remove(file_path)
except Exception as e:
    logging.error(f"Erreur: {str(e)}")