import requests

def fetch_power_breakdown_data(zone="MA"):
    """
    Récupère les données de décomposition de la puissance de l'API ElectricityMap pour une zone spécifique.
    
    :param zone: La zone pour laquelle vous voulez obtenir les données (par exemple, "MA" pour le Maroc).
    :return: Un dictionnaire contenant les données de décomposition de la puissance.
    """
    base_url = "https://api-access.electricitymaps.com/free-tier/power-breakdown/latest"
    
    # Inclure la zone dans les paramètres de la requête
    params = {'zone': zone}
    
    # Remplacez "YOUR_API_KEY" par votre clé d'API
    api_key = "oQWn2xj45ukPWC3v1lMWJclIY43ngkKY"

    try:
        # Inclure la clé d'API dans les en-têtes de la requête
        headers = {'Authorization': f'Token {api_key}'}
        response = requests.get(base_url, params=params, headers=headers)
        data = response.json()
        
        return data
    except requests.exceptions.RequestException as e:
        print(f"Erreur lors de la requête : {e}")
        return None

# Exemple d'utilisation
power_breakdown_data = fetch_power_breakdown_data("MA")
if power_breakdown_data:
    print("Données de décomposition de la puissance pour la zone MA:")
    print(power_breakdown_data)
