import dash
import json
import plotly.express as px
from dash import dcc, html
from dash.dependencies import Input, Output
import pandas as pd
from pymongo import MongoClient

# Connexion à MongoDB
client = MongoClient("mongodb://localhost:27017")
db = client["weatherElectro"]
collection = db["weather_electricity_data"]

# Charger les données depuis MongoDB
data = pd.DataFrame(list(collection.find()))

# Convertir la colonne datetime en format datetime
data['datetime'] = pd.to_datetime(data['datetime'])

# Calculer la durée d'ensoleillement pour chaque ligne
data['sunshine_duration'] = (data['sunset'] - data['sunrise']).dt.total_seconds()

# Extraire "main" de la colonne "weather" pour chaque ville
data['weather_main'] = data['weather'].apply(lambda x: json.loads(x)[0]['main'] if pd.notna(x) else None)


# Initialiser l'application Dash
app = dash.Dash(__name__)

# Définir la mise en page de l'application
app.layout = html.Div(children=[
    html.H1(children='Analyse Météo et Électricité'),

    # Analyse de la tendance météorologique par ville
    dcc.Graph(
        id='weather-trend',
        figure=px.line(
            data,
            x='datetime',
            y='temperature',
            color='city_name',
            title='Évolution de la température par ville',
            labels={
                'value': 'Température (K)',
                'variable': 'Paramètres',
                'city_name': 'Ville',
            }
        )
    ),

    # Analyse de la durée d'ensoleillement moyen et de la vitesse moyenne du vent par ville
    dcc.Graph(
        id='sunshine-and-wind',
        figure=px.bar(
            data.groupby('city_name').agg({
                'sunshine_duration': 'mean',
                'wind_speed': 'mean'
            }).reset_index(),
            x='city_name',
            y=['sunshine_duration', 'wind_speed'],
            title='Durée d\'ensoleillement moyen et Vitesse moyenne du vent par ville',
            labels={'value': 'Durée d\'ensoleillement moyen (secondes) / Vitesse moyenne du vent (m/s)',
                    'variable': 'Mesures', 'city_name': 'Ville'},
            barmode='group',  # Utilisez 'group' pour afficher les barres côte à côte
        )
    ),

    # Analyse de la corrélation entre conditions météorologiques et consommation d'électricité pour Tanger
    dcc.Graph(
        id='correlation-weather-electricity-tanger',
        figure={
            'data': [
                {'x': data[data['city_name'] == 'Tangier']['datetime'],
                 'y': data[data['city_name'] == 'Tangier']['electricity_value'], 'mode': 'lines',
                 'name': 'Consommation d\'électricité (MW)'},
                {'x': data[data['city_name'] == 'Tangier']['datetime'],
                 'y': data[data['city_name'] == 'Tangier']['temperature'], 'mode': 'lines',
                 'name': 'Température (K)'},
                {'x': data[data['city_name'] == 'Tangier']['datetime'],
                 'y': data[data['city_name'] == 'Tangier']['pressure'], 'mode': 'lines',
                 'name': 'Pression'},
                {'x': data[data['city_name'] == 'Tangier']['datetime'],
                 'y': data[data['city_name'] == 'Tangier']['wind_speed'], 'mode': 'lines',
                 'name': 'Vitesse du vent'},
                {'x': data[data['city_name'] == 'Tangier']['datetime'],
                 'y': data[data['city_name'] == 'Tangier']['humidity'], 'mode': 'lines',
                 'name': 'Humidité'},
            ],
            'layout': {
                'title': 'Corrélation entre Température, Pression, Vitesse du Vent, Humidité, Visibilité et Consommation d\'électricité - Tanger',
                'xaxis': {'title': 'Date et Heure'},
                'yaxis': {'title': 'Conditions Météorologiques'}
            }
        }
    ),

    # Analyse des segments de consommateurs
    dcc.Graph(
        id='consumer-segments',
        figure={
            'data': [
                {'x': data['city_name'], 'y': data['electricity_value'], 'type': 'bar',
                 'name': 'Consommation totale'},
                {'x': data['city_name'], 'y': data['consommation_moyenne_domicile'], 'type': 'bar',
                 'name': 'Consommation moyenne domicile'},
                {'x': data['city_name'], 'y': data['consommation_moyenne_entreprises'], 'type': 'bar',
                 'name': 'Consommation moyenne entreprises'},
                {'x': data['city_name'], 'y': data['consommation_moyenne_industries'], 'type': 'bar',
                 'name': 'Consommation moyenne industries'},
            ],
            'layout': {
                'title': 'Consommation d\'électricité par type de consommateur et par ville',
                'xaxis': {'title': 'Ville'},
                'yaxis': {'title': 'Consommation d\'électricité (MW)'}
            }
        }
    ),

    # Graphique des conditions météorologiques par ville
    dcc.Graph(
        id='weather-conditions',
        figure=px.bar(
            data.explode('weather').reset_index(),
            x='city_name',
            y='weather_main',
            color='city_name',
            title='Conditions météorologiques par ville',
            labels={'value': 'Valeur', 'variable': 'Conditions météorologiques', 'city_name': 'Ville'},
            barmode='stack',  # Utilisez 'stack' pour empiler les barres
        )
    ),

    # Interval pour actualiser les données toutes les 10 secondes
    dcc.Interval(
        id='interval-component',
        interval=10 * 1000,  # in milliseconds
        n_intervals=0
    )
])


# Callback pour la mise à jour des données à intervalles réguliers
@app.callback(
    Output('weather-trend', 'figure'),
    Output('sunshine-and-wind', 'figure'),
    Output('correlation-weather-electricity-tanger', 'figure'),
    Output('consumer-segments', 'figure'),
    Output('weather-conditions', 'figure'),
    Input('interval-component', 'n_intervals'),
    prevent_initial_call=True
)
def update_data(n):
    # Charger les nouvelles données depuis MongoDB
    new_data = pd.DataFrame(list(collection.find()))

    # Convertir la colonne datetime en format datetime
    new_data['datetime'] = pd.to_datetime(new_data['datetime'])
    
    # Extraire "main" de la colonne "weather" pour chaque ville
    new_data['weather_main'] = new_data['weather'].apply(lambda x: json.loads(x)[0]['main'] if pd.notna(x) else None)

    # Calculer la durée d'ensoleillement pour chaque ligne
    new_data['sunshine_duration'] = (new_data['sunset'] - new_data['sunrise']).dt.total_seconds()

    # Mise à jour des graphiques avec les nouvelles données
    fig_weather_trend = px.line(
        new_data,
        x='datetime',
        y='temperature',
        color='city_name',
        title='Évolution des paramètres météorologiques et de la consommation par ville',
        labels={
            'value': 'Valeur',
            'variable': 'Paramètres',
            'city_name': 'Ville',
            'temperature': 'Température (K)'
        }
    )

    fig_sunshine_and_wind = px.bar(
        new_data.groupby('city_name').agg({
            'sunshine_duration': 'mean',
            'wind_speed': 'mean'
        }).reset_index(),
        x='city_name',
        y=['sunshine_duration', 'wind_speed'],
        title='Durée d\'ensoleillement moyen et Vitesse moyenne du vent par ville',
        labels={'value': 'Durée d\'ensoleillement moyen (secondes) / Vitesse moyenne du vent (m/s)',
                'variable': 'Mesures', 'city_name': 'Ville'},
        barmode='group',  # Utilisez 'group' pour afficher les barres côte à côte
    )

    fig_correlation_weather_electricity_tanger = {
        'data': [
            {'x': new_data[new_data['city_name'] == 'Tangier']['datetime'],
             'y': new_data[new_data['city_name'] == 'Tangier']['electricity_value'], 'mode': 'lines',
             'name': 'Consommation d\'électricité (MW)'},
            {'x': new_data[new_data['city_name'] == 'Tangier']['datetime'],
             'y': new_data[new_data['city_name'] == 'Tangier']['temperature'], 'mode': 'lines',
             'name': 'Température (K)'},
            {'x': new_data[new_data['city_name'] == 'Tangier']['datetime'],
             'y': new_data[new_data['city_name'] == 'Tangier']['pressure'], 'mode': 'lines',
             'name': 'Pression'},
            {'x': new_data[new_data['city_name'] == 'Tangier']['datetime'],
             'y': new_data[new_data['city_name'] == 'Tangier']['wind_speed'], 'mode': 'lines',
             'name': 'Vitesse du vent'},
            {'x': new_data[new_data['city_name'] == 'Tangier']['datetime'],
             'y': new_data[new_data['city_name'] == 'Tangier']['humidity'], 'mode': 'lines',
             'name': 'Humidité'},
            {'x': new_data[new_data['city_name'] == 'Tangier']['datetime'],
             'y': new_data[new_data['city_name'] == 'Tangier']['feels_like'], 'mode': 'lines',
             'name': 'Temperature ressentie'},
            {'x': new_data[new_data['city_name'] == 'Tangier']['datetime'],
             'y': new_data[new_data['city_name'] == 'Tangier']['cloudiness'], 'mode': 'lines',
             'name': 'nuages'},
        ],
        'layout': {
            'title': 'Corrélation entre Température, Pression, Vitesse du Vent, Humidité, Visibilité et Consommation d\'électricité - Tanger',
            'xaxis': {'title': 'Date et Heure'},
            'yaxis': {'title': 'Conditions Météorologiques'}
        }
    }

    fig_consumer_segments = {
        'data': [
            {'x': new_data['city_name'], 'y': new_data['electricity_value'], 'type': 'bar',
             'name': 'Consommation totale'},
            {'x': new_data['city_name'], 'y': new_data['consommation_moyenne_domicile'], 'type': 'bar',
             'name': 'Consommation moyenne domicile'},
            {'x': new_data['city_name'], 'y': new_data['consommation_moyenne_entreprises'], 'type': 'bar',
             'name': 'Consommation moyenne entreprises'},
            {'x': new_data['city_name'], 'y': new_data['consommation_moyenne_industries'], 'type': 'bar',
             'name': 'Consommation moyenne industries'},
        ],
        'layout': {
            'title': 'Consommation d\'électricité par type de consommateur et par ville',
            'xaxis': {'title': 'Ville'},
            'yaxis': {'title': 'Consommation d\'électricité (MW)'}
        }
    }

    # Créer un graphique à barres pour afficher le temps principal par ville
    fig_weather_conditions = px.bar(data, x='city_name', y='weather_main', color='weather_main',
             title='Conditions météorologiques principales par ville',
             labels={'weather_main': 'Conditions météorologiques principales', 'city_name': 'Ville'},
             barmode='stack')  # Utilisez 'stack' pour empiler les barres
    
    return fig_weather_trend, fig_sunshine_and_wind, fig_correlation_weather_electricity_tanger, fig_consumer_segments, fig_weather_conditions


if __name__ == '__main__':
    app.run_server(debug=True)
