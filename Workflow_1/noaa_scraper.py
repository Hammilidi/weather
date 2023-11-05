import requests
from bs4 import BeautifulSoup
import os
import gzip

BASE_URL = "https://www1.ncdc.noaa.gov/pub/data/noaa"

# Créez un répertoire pour stocker les fichiers téléchargés
download_directory = "noaa_world_stations_data"
os.makedirs(download_directory, exist_ok=True)

# Parcourez les années de 2000 à 2023
for year in range(2000, 2024):
    # Créez l'URL de l'année
    year_url = f"{BASE_URL}/{year}/"

    # Obtenez le contenu HTML de la page
    response = requests.get(year_url)
    if response.status_code == 200:
        # Analysez le contenu HTML avec BeautifulSoup
        soup = BeautifulSoup(response.text, "html.parser")

        # Parcourez les liens sur la page
        for link in soup.find_all("a"):
            file_url = link.get("href")
            if file_url.endswith(".gz"):
                # Téléchargez le fichier
                file_name = file_url.split("/")[-1]
                file_path = os.path.join(download_directory, file_name)
                file_url = f"{year_url}{file_url}"
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
    else:
        print(f"Impossible d'accéder à {year_url}")

print("Téléchargement et décompression terminés.")
