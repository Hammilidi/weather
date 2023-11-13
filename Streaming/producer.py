import requests
import time
from confluent_kafka import Producer

# Remplacez 'YOUR_API_KEY' par votre clé API OpenWeather
api_key = '37e333ac92209e27137831ba501a0c03'

TOPIC_NAME = 'weather-data'

# Liste de villes au Maroc pour lesquelles vous souhaitez obtenir des données météorologiques
cities = ['Casablanca', 'Rabat', 'Marrakech', 'Fes', 'Tangier']

# Code de pays pour le Maroc
country_code = 'MA'

# Configuration du producteur Kafka
producer_config = {
    'bootstrap.servers': 'localhost:9092',  # Adresse du broker Kafka
    'client.id': 'openweather-producer'
}


# Créez une instance du producteur Kafka
producer = Producer(producer_config)

# Dictionnaire pour stocker les dernières données météorologiques par ville
last_weather_data = {}

while True:
    # Parcourez la liste de villes et effectuez des requêtes pour chaque ville
    for city in cities:
        # URL de l'endpoint de l'API pour les données météorologiques actuelles
        url = f'http://api.openweathermap.org/data/2.5/weather?q={city},{country_code}&appid={api_key}'

        # Effectuez la requête HTTP pour obtenir les données météorologiques actuelles
        response = requests.get(url)

        # Vérifiez la réponse
        if response.status_code == 200:
            data = response.json()
            # print(data)
            # Les données météorologiques actuelles sont stockées dans 'data'
            # print(f'Données météorologiques pour {city}:')
            # print('Température (en °C):', data['main']['temp'])
            # print('Conditions météorologiques:', data['weather'][0]['description'])
            # print('Humidité:', data['main']['humidity'], '%')
            # print('Vitesse du vent (en m/s):', data['wind']['speed'])
            # print('\n')
            producer.produce(TOPIC_NAME, key=city, value=str(data))
            producer.flush()
            print("Message envoyé à Kafka !")
            time.sleep(5)
        else:
            print(f'Erreur pour {city}: {response.status_code}')
