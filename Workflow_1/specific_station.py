import requests
import os
import gzip

# Spécifiez l'identifiant de la station OACI que vous souhaitez télécharger.
station_oaci = "GMTT"  # Exemple : Aéroport de Toulouse-Blagnac

# Créez l'URL de la station en utilisant le code OACI
BASE_URL = "https://www1.ncdc.noaa.gov/pub/data/noaa"
station_url = f"{BASE_URL}/{station_oaci}/"

# Créez un répertoire pour stocker les fichiers téléchargés
download_directory = "station_data"
os.makedirs(download_directory, exist_ok=True)

# Obtenez le contenu HTML de la page
response = requests.get(station_url)
if response.status_code == 200:
    # Parcourez les liens sur la page
    for file_url in response.text.split('\n'):
        if file_url.endswith(".gz"):
            # Téléchargez le fichier
            file_name = file_url.split("/")[-1]
            file_path = os.path.join(download_directory, file_name)
            file_url = f"{station_url}{file_url}"
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
    print(f"Impossible d'accéder à {station_url}")

print("Téléchargement et décompression terminés.")
