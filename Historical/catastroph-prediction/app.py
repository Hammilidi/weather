import pandas as pd
import streamlit as st
import joblib
from datetime import datetime

# Charger le modèle enregistré
model = joblib.load('bagging_model.joblib')

# Interface Streamlit avec mise en forme améliorée
st.title("Prédictions de Pluie et Alertes Météo")

st.image("/home/FIL_ROUGE/media/rain.jpg", width=700)



# Ajouter des champs de saisie pour les caractéristiques d'entrée
hour = st.slider("Heure", 0, 23, 12)
day_of_week = st.slider("Jour de la semaine", 0, 6, 3)
month = st.slider("Mois", 1, 12, 6)
temperature_2m = st.slider("Temperature", 60.0)
relativehumidity_2m = st.slider("Humidite", 60.0)

# Préparer les caractéristiques d'entrée pour la prédiction
input_features = pd.DataFrame({
    'temperature_2m (°C)': [temperature_2m],
    'relativehumidity_2m (%)': [relativehumidity_2m],
    'dewpoint_2m (°C)': [15.0],
    'apparent_temperature (°C)': [19.0],
    'precipitation (mm)': [5.0],
    'snowfall (cm)': [0.0],
    'snow_depth (m)': [0.1],
    'weathercode (wmo code)': [800],
    'pressure_msl (hPa)': [1015.0],
    'surface_pressure (hPa)': [1010.0],
    'cloudcover (%)': [40.0],
    'cloudcover_low (%)': [20.0],
    'cloudcover_mid (%)': [15.0],
    'cloudcover_high (%)': [5.0],
    'et0_fao_evapotranspiration (mm)': [4.0],
    'vapor_pressure_deficit (kPa)': [0.8],
    'windspeed_10m (m/s)': [3.0],
    'windspeed_100m (m/s)': [8.0],
    'winddirection_10m (°)': [180.0],
    'winddirection_100m (°)': [190.0],
    'windgusts_10m (m/s)': [5.0],
    'soil_temperature_0_to_7cm (°C)': [18.0],
    'soil_temperature_7_to_28cm (°C)': [15.0],
    'soil_temperature_28_to_100cm (°C)': [12.0],
    'soil_temperature_100_to_255cm (°C)': [10.0],
    'soil_moisture_0_to_7cm (m³/m³)': [0.2],
    'soil_moisture_7_to_28cm (m³/m³)': [0.3],
    'soil_moisture_28_to_100cm (m³/m³)': [0.25],
    'soil_moisture_100_to_255cm (m³/m³)': [0.15],
    'shortwave_radiation (W/m²)': [200.0],
    'direct_radiation (W/m²)': [150.0],
    'diffuse_radiation (W/m²)': [50.0],
    'direct_normal_irradiance (W/m²)': [180.0],
    'terrestrial_radiation (W/m²)': [160.0],
    'shortwave_radiation_instant (W/m²)': [210.0],
    'direct_radiation_instant (W/m²)': [160.0],
    'diffuse_radiation_instant (W/m²)': [50.0],
    'direct_normal_irradiance_instant (W/m²)': [190.0],
    'terrestrial_radiation_instant (W/m²)': [170.0],
    'hour': [hour],
    'day_of_week': [day_of_week],
    'month': [month]
})

# Faire la prédiction
predicted_rain = model.predict(input_features)

# Afficher la prédiction
st.subheader("Prédiction de la quantité de pluie :")
st.write(f"La quantité de pluie prévue est de {predicted_rain[0]:.2f} mm")

# Recréer les caractéristiques pour le système d'alerte
input_features_for_alerts = input_features.copy()

# Déclencher des alertes en fonction des seuils (ajuster les seuils selon votre logique)
seuil_secheresse = 100
seuil_inondation = 500

alert_secheresse = predicted_rain[0] < seuil_secheresse
alert_inondation = predicted_rain[0] > seuil_inondation

# Afficher les alertes
st.subheader("Alertes Météo")
st.write(f"Alerte de sécheresse : {alert_secheresse}")
st.write(f"Alerte d'inondation : {alert_inondation}")
