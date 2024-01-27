from confluent_kafka import Producer
import requests
import json
import time  # Add this import statement


# Configuration du producteur Kafka
conf = {
    'bootstrap.servers': 'localhost:9092',  # Adresse des brokers Kafka
}

producer = Producer(conf)

# URL de ton API Flask
api_url = 'http://127.0.0.1:5000/consommation-electricite'

# Fonction pour envoyer les données générées vers Kafka
def send_to_kafka():
    try:
        # Appel à l'API pour récupérer les données de consommation
        response = requests.get(api_url)
        if response.status_code == 200:
            data = response.json()


            time.sleep(10)
            # Envoi des données au topic Kafka
            producer.produce('electricity_consumption', key='cle', value=json.dumps(data))
            producer.flush()
            print("Données envoyées avec succès à Kafka :", data)
            print("\n")
            # print("Message envoyé  à Kafka")
    except Exception as e:
        print("Erreur lors de l'envoi des données à Kafka :", e)

# Appel à la fonction d'envoi des données à intervalles réguliers
while True:
    send_to_kafka()
