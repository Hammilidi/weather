import dash
import pandas as pd
from dash import dcc, html
import plotly.express as px
from pymongo import MongoClient

# Initialisation de l'application Dash
app = dash.Dash(__name__)

# Connexion à MongoDB
client = MongoClient("mongodb://localhost:27017")
db_name = "weather"
collection_name = "weather_data"
db = client[db_name]
collection = db[collection_name]

# Métriques à afficher
def get_average_temperature_by_city():
    pipeline = [
        {"$group": {"_id": "$city_name", "avg_temp": {"$avg": "$temperature"}}}
    ]
    result = list(collection.aggregate(pipeline))
    return result

def get_weather_data_for_city(city):
    try:
        # Récupération des données pour la ville spécifiée
        cursor = collection.find({"city_name": city})

        # Création d'un DataFrame Pandas pour stocker les données
        data = pd.DataFrame(list(cursor))

        # Remplir les valeurs manquantes avec 0
        data = data.fillna(0)

        # Vérifier et aligner les longueurs des colonnes
        columns_to_check = ['datetime', 'temperature', 'min_temperature', 'max_temperature', 'feels_like']
        for col in columns_to_check:
            if col not in data.columns:
                data[col] = 0  # Ajouter la colonne manquante et la remplir avec 0

        return data

    except Exception as e:
        print(f"Failed to fetch data for {city}: {e}")
        return pd.DataFrame()  # Retourner un DataFrame vide en cas d'échec


# Layout de l'application Dash
app.layout = html.Div([
    dcc.Graph(
        id='average-temperature-by-city',
        figure=px.bar(get_average_temperature_by_city(), x='_id', y='avg_temp', title='Température moyenne par ville')
    ),
    dcc.Graph(
        id='temperature-evolution-rabat',
        figure=px.line(get_weather_data_for_city('Rabat'), x='datetime', y=['temperature', 'min_temperature', 'max_temperature', 'feels_like'], title='Évolution de la température pour Rabat')
    ),
    # Ajouter d'autres graphiques ici
])

if __name__ == '__main__':
    app.run_server(debug=True)
