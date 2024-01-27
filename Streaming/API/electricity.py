from flask import Flask, jsonify
import random
import threading
import time
import schedule

app = Flask(__name__)
cities = ['Tangier', 'Rabat', 'Fes', 'Casablanca', 'Marrakesh']
city_index = 0
cities_data = {}

# Fonction pour générer des données d'électricité pour une ville spécifique
def generate_electricity_data_for_city():
    global cities_data, city_index
    city = cities[city_index % len(cities)]
    consumption = random.uniform(50, 200)
    city_data = {
        "zoneKey": f"{city}",
        "countryCode": "MA",
        "consommation": {
            "valeur": consumption,
            "unités": "MW"
        },
        "production": [
            {
                "source": "solaire",
                "valeur": random.uniform(10, 50),
                "unités": "MW"
            },
            {
                "source": "éolien",
                "valeur": random.uniform(5, 30),
                "unités": "MW"
            },
            {
                "source": "nucléaire",
                "valeur": random.uniform(30, 100),
                "unités": "MW"
            },
            # Autres sources de production
        ],
        "timestamp": time.time(),
        "utilisateur": {
            "nombre_utilisateurs": random.randint(1000, 5000),
            "type_utilisateurs": {
                "domicile": random.randint(400, 2000),
                "entreprises": random.randint(200, 1000),
                "industries": random.randint(100, 500),
                # Autres catégories d'utilisateurs que vous souhaitez inclure
            },
            "comportement": {
                "pic_heure": random.randint(0, 23),
                "pic_valeur": random.uniform(100, 300),
                "consommation_moyenne": {
                    "domicile": random.uniform(20, 80),
                    "entreprises": random.uniform(50, 200),
                    "industries": random.uniform(100, 300),
                    # Autres catégories de consommation moyenne
                }
            }
        }
    }
    cities_data[city] = city_data
    city_index += 1

# Planifier la génération de données toutes les secondes pour une ville différente
schedule.every(1).seconds.do(generate_electricity_data_for_city)

# Fonction pour récupérer les nouvelles données d'électricité pour la prochaine ville
@app.route('/consommation-electricite', methods=['GET'])
def get_next_city_data():
    global cities_data, city_index
    city = cities[city_index % len(cities)]
    city_index += 1
    return jsonify(cities_data.get(city, {}))


# Fonction pour exécuter la planification en arrière-plan
def run_schedule():
    while True:
        schedule.run_pending()
        time.sleep(1)

# Lancer le serveur Flask dans un thread
if __name__ == '__main__':
    thread = threading.Thread(target=run_schedule)
    thread.start()
    app.run(debug=True)
