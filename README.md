# Desciption
Ce projet vise à effectuer une analyse des données de la météo. Les objectifs sont les suivants:
* Identifier et analyser les tendances climatiques 
* L'influence du climat sur les sols et l'agriculture
* Analyser la pollution/qualité de l'air
* Analyser le potentiel d'energie solaire
* Implementer un système de prédiction des précipitation avec un système d'alerte de catastrophes climatiques (inondation, secheresse)

# Architecture du projet
![project architecture](/media/architecture.png)

# Préréquis
Pour réaliser ou executer ce projet, voilà le stack de technologies utilisées:
#### Historical 
* Requets
* BeautifulSoup 
* Pandas 
* Numpy 
* Microsoft SQL Server 
* tSQLt 
* pytest
* MongoDB 
* PowerBI
* scikit-learn 
* Streamlit 
* Apache Airflow
#### Streaming 
- Apache Kafka
- Apache Spark
- Cassandra
- Python Dash
- Flask

#### Config & Operating System
- Docker
- Linux

# Project Directory Structure

## Weather Analysis

- `Historical/`

  - `airflow/`
    - `dag.py`

  - `Caches/`
    - `.air.sqlite`
    - `.openmeteo.sqlite`

  - `catastroph-prediction/`

  - `data/`
    - `processed/`
    - `raw/`
      - `air/`
      - `noaa_world_stations_data/`
      - `openmeteo/`

  - `data-ingestion/`
    - `airquality.py`
    - `noaa_tanger.py`
    - `openmeteo.py`
    - `world_noaa.py`

  - `Log/`
    - `air.log`
    - `noaa_tanger.log`
    - `openmeteo.log`

  - `transformations/`
  - `unitTests/`
    - `test.py`

- `Streaming/`
  - `analysis/`
    - `dashboard.py`

  - `forecast-system/`
    - `forecast-api.py`

  - `kafka/`
    - `Logs/`
       - `producer.log`
       - `consumer.log`


    - `consumer.py`
    - `producer.py`


- `media/`
  - `HistoricalMedia/`
  - `StreamMedia/`

  
- `config/`
   - `docker-compose.yml`


- `README.md`


```
echo "# weather" >> README.md
git init
git add README.md
git commit -m "first commit"
git branch -M data-collection
git remote add origin https://github.com/Hammilidi/weather.git
git push -u origin data-collection
git checkout data-collection
```

