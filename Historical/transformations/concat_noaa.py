import os
import pandas as pd

# Chemin vers le dossier contenant les fichiers METAR
folder_path = "/home/FIL_ROUGE/Historical/data/raw/noaa_tanger/"

# Créez une liste vide pour stocker les données
data = []

# Parcourez les fichiers dans le dossier
for filename in os.listdir(folder_path):
    file_path = os.path.join(folder_path, filename)
    
    with open(file_path, 'r', encoding='utf-8', errors="ignore") as file:
        for line in file:
            # Divisez la ligne en champs en utilisant des espaces comme délimiteurs
            fields = line.split()
            if len(fields) < 16:
                continue  # Ignorez les lignes qui n'ont pas tous les champs attendus
            
            code_oaci = fields[0]
            type_station = fields[1]
            date_heure_observation = fields[2]
            latitude = fields[3]
            longitude = fields[4]
            type_rapport = fields[5]
            indicateur_correction = fields[6]
            vent = fields[7]
            visibilite = fields[8]
            phenomenes_meteo = fields[9]
            nuages = " ".join(fields[10:-6])
            temperature = fields[-6]
            pression_atmospherique = fields[-5]
            indicateur_temps = fields[-4]
            inconnu_ou_autres_informations = " ".join(fields[-3:])
            
            # Ajoutez les données à la liste
            data.append([
                code_oaci,
                type_station,
                date_heure_observation,
                latitude,
                longitude,
                type_rapport,
                indicateur_correction,
                vent,
                visibilite,
                phenomenes_meteo,
                nuages,
                temperature,
                pression_atmospherique,
                indicateur_temps,
                inconnu_ou_autres_informations
            ])

# Créez un DataFrame à partir de la liste de données
df = pd.DataFrame(data, columns=[
    "Code_OACI", "Type_Station", "Date_Heure_Observation", "Latitude", "Longitude",
    "Type_Rapport", "Indicateur_Correction", "Vent", "Visibilite", "Phenomenes_Meteo",
    "Nuages", "Temperature", "Pression_Atmospherique", "Indicateur_Temps", "Inconnu_Ou_Autres_Informations"
])

# Exportez le DataFrame en tant que fichier CSV
df.to_csv("/home/FIL_ROUGE/Historical/data/processed/nooa_data.csv", index=False)

print("Le fichier CSV a été exporté avec succès.")