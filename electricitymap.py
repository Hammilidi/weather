import requests

def fetch_electricity_map_data(zone="FR"):
    """
    Récupère les données de l'API ElectricityMap pour une zone spécifique.
    
    :param zone: La zone pour laquelle vous voulez obtenir les données (par exemple, "FR" pour la France).
    :return: Un dictionnaire contenant les données de consommation, d'émission de CO2, et de production d'énergie.
    """
    base_url = "https://api.electricitymap.org/v3/zones"
    url = f"{base_url}/{zone}"

    try:
        response = requests.get(url)
        data = response.json()
        
        if 'zoneKey' in data:
            # Les données sont disponibles pour cette zone
            consumption = data['consumption']['current']
            carbon_intensity = data['carbonIntensity']['current']
            production = data['production']
            
            return {
                'zone': zone,
                'consumption': consumption,
                'carbon_intensity': carbon_intensity,
                'production': production
            }
        else:
            print(f"Données non disponibles pour la zone {zone}")
            return None
    except requests.exceptions.RequestException as e:
        print(f"Erreur lors de la requête : {e}")
        return None

# Exemple d'utilisation
zone_data = fetch_electricity_map_data("MA")
if zone_data:
    print("Zone:", zone_data['zone'])
    print("Consommation:", zone_data['consumption'], "MW")
    print("Intensité carbone:", zone_data['carbon_intensity'], "gCO2eq/kWh")
    print("Production d'énergie:")
    for source, value in zone_data['production'].items():
        print(f"{source}: {value} MW")
