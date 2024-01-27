from pyspark.sql import SparkSession, Row
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, ArrayType, IntegerType, MapType, FloatType, TimestampType
from pyspark.sql.functions import from_json, col, expr, from_unixtime, date_format
from pymongo import MongoClient, UpdateOne
from pymongo.errors import ConnectionFailure, CollectionInvalid



# Initialize Spark session
spark = SparkSession.builder \
    .appName("weatherElectricity") \
    .config("spark.streaming.stopGracefullyOnShutdown", True) \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,com.datastax.spark:spark-cassandra-connector_2.12:3.2.0") \
    .config("spark.executor.memory", "2g") \
    .config("spark.driver.memory", "2g") \
    .getOrCreate()

# Read from Kafka for weather data
kafkaStreamWeather = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "weather-data") \
    .load()

# Define the StructType for the weather data
weather_schema = StructType([
    StructField("coord", StructType([
        StructField("lon", FloatType(), True),
        StructField("lat", FloatType(), True)
    ]), True),
    StructField("weather", StringType(), True),
    StructField("base", StringType(), True),
    StructField("main", StructType([
        StructField("temp", FloatType(), True),
        StructField("feels_like", FloatType(), True),
        StructField("temp_min", FloatType(), True),
        StructField("temp_max", FloatType(), True),
        StructField("pressure", IntegerType(), True),
        StructField("humidity", IntegerType(), True)
    ]), True),
    StructField("visibility", IntegerType(), True),
    StructField("wind", StructType([
        StructField("speed", FloatType(), True),
        StructField("deg", IntegerType(), True)
    ]), True),
    StructField("clouds", StructType([
        StructField("all", IntegerType(), True)
    ]), True),
    StructField("dt", IntegerType(), True),
    StructField("sys", StructType([
        StructField("type", IntegerType(), True),
        StructField("id", IntegerType(), True),
        StructField("country", StringType(), True),
        StructField("sunrise", IntegerType(), True),
        StructField("sunset", IntegerType(), True)
    ]), True),
    StructField("timezone", IntegerType(), True),
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("cod", IntegerType(), True)
])

# Parse JSON from Kafka for weather data
parsed_stream_weather = kafkaStreamWeather.selectExpr("CAST(value AS STRING)")
df_weather = parsed_stream_weather.withColumn("weather_values", from_json(parsed_stream_weather["value"], weather_schema))
df_weather = df_weather.select("weather_values.*")


# Read from Kafka for electricity consumption data
kafkaStreamElectricity = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "electricity_consumption") \
    .load()

# Define the StructType for the electricity consumption data
electricity_schema = StructType([
    StructField("consommation", StructType([
        StructField("unités", StringType(), True),
        StructField("valeur", DoubleType(), True)
    ]), True),
    StructField("countryCode", StringType(), True),
    StructField("production", ArrayType(StructType([
        StructField("source", StringType(), True),
        StructField("unités", StringType(), True),
        StructField("valeur", DoubleType(), True)
    ])), True),
    StructField("timestamp", DoubleType(), True),
    StructField("utilisateur", StructType([
        StructField("comportement", StructType([
            StructField("consommation_moyenne", StructType([
                StructField("domicile", DoubleType(), True),
                StructField("entreprises", DoubleType(), True),
                StructField("industries", DoubleType(), True)
            ]), True),
            StructField("pic_heure", IntegerType(), True),
            StructField("pic_valeur", DoubleType(), True)
        ]), True),
        StructField("nombre_utilisateurs", IntegerType(), True),
        StructField("type_utilisateurs", StructType([
            StructField("domicile", IntegerType(), True),
            StructField("entreprises", IntegerType(), True),
            StructField("industries", IntegerType(), True)
        ]), True)
    ]), True),
    StructField("zoneKey", StringType(), True)
])

# Parse JSON from Kafka for electricity consumption data
parsed_stream_electricity = kafkaStreamElectricity.selectExpr("CAST(value AS STRING)")
df_electricity = parsed_stream_electricity.withColumn("electricity_values", from_json(parsed_stream_electricity["value"], electricity_schema))
df_electricity = df_electricity.select("electricity_values.*")

# Join weather data with electricity consumption data by city
joined_data = df_weather.join(df_electricity, df_weather["name"] == df_electricity["zoneKey"], "inner")

df = joined_data.select(
    col("weather").alias("weather"),
    col("base").alias("base"),
    col("main.temp").alias("temperature"),
    col("main.feels_like").alias("feels_like"),
    col("main.temp_min").alias("min_temperature"),
    col("main.temp_max").alias("max_temperature"),
    col("main.pressure").alias("pressure"),
    col("main.humidity").alias("humidity"),
    col("visibility").alias("visibility"),
    col("wind.speed").alias("wind_speed"),
    col("wind.deg").alias("wind_degree"),
    col("clouds.all").alias("cloudiness"),
    col("dt").alias("datetime"),
    col("sys.type").alias("sys_type"),
    col("sys.id").alias("sys_id"),
    col("sys.country").alias("country"),
    col("sys.sunrise").alias("sunrise"),
    col("sys.sunset").alias("sunset"),
    col("timezone").alias("timezone"),
    col("id").alias("city_id"),
    col("name").alias("city_name"),
    col("cod").alias("cod"),
    col("consommation.unités").alias("electricity_units"),
    col("consommation.valeur").alias("electricity_value"),
    col("countryCode").alias("electricity_country"),
    col("production").alias("electricity_production"),
    col("utilisateur.comportement.consommation_moyenne.domicile").alias("consommation_moyenne_domicile"),
    col("utilisateur.comportement.consommation_moyenne.entreprises").alias("consommation_moyenne_entreprises"),
    col("utilisateur.comportement.consommation_moyenne.industries").alias("consommation_moyenne_industries"),
    col("utilisateur.comportement.pic_heure").alias("pic_heure"),
    col("utilisateur.comportement.pic_valeur").alias("pic_valeur"),
    col("utilisateur.type_utilisateurs.domicile").alias("domicile"),
    col("utilisateur.type_utilisateurs.entreprises").alias("entreprises"),
    col("utilisateur.type_utilisateurs.industries").alias("industries"),
)

df = df.withColumn("datetime", from_unixtime(col("datetime")).cast(TimestampType()))
df = df.withColumn("sunrise", from_unixtime(col("sunrise")).cast(TimestampType()))
df = df.withColumn("sunset", from_unixtime(col("sunset")).cast(TimestampType()))


# print(df.dtypes)

# # Write the data to console
# streaming_query = df.writeStream \
#     .format("console") \
#     .outputMode("append") \
#     .option("checkpointLocation", "./checkpoint/data") \
#     .start()

# streaming_query.awaitTermination()  # Wait for the processing to finish


# # # #--------------------------------------------------MongoDB---------------------------------------------------------------


def save_to_mongodb(batch_df, batch_id):
    try:
        client = MongoClient("mongodb://localhost:27017")
        db_name = "weatherElectro"
        collection_name = "weather_electricity_data"

        db = client[db_name]
        collection = db[collection_name]

        update_requests = []

        for row in batch_df.rdd.map(lambda r: r.asDict()).collect():
            data = {
                "weather": row["weather"],
                "temperature": row["temperature"],
                "feels_like": row["feels_like"],
                "pressure": row["pressure"],
                "humidity": row["humidity"],
                "visibility": row["visibility"],
                "wind_speed": row["wind_speed"],
                "wind_degree": row["wind_degree"],
                "cloudiness": row["cloudiness"],
                "datetime": row["datetime"],
                "country": row["country"],
                "sunrise": row["sunrise"],
                "sunset": row["sunset"],
                "city_name": row["city_name"],
                "electricity_units": row["electricity_units"],
                "electricity_value": row["electricity_value"],
                "electricity_production": row["electricity_production"],
                "consommation_moyenne_domicile": row["consommation_moyenne_domicile"],
                "consommation_moyenne_entreprises": row["consommation_moyenne_entreprises"],
                "consommation_moyenne_industries": row["consommation_moyenne_industries"],
                "pic_heure": row["pic_heure"],
                "pic_valeur": row["pic_valeur"],
                "nb_domicile": row["domicile"],
                "nb_entreprises": row["entreprises"],
                "nb_industries": row["industries"],
            }

            # Utiliser le datetime comme filtre d'identification unique
            filter_condition = {"datetime": data["datetime"]}

            update_requests.append(
                UpdateOne(filter_condition, {"$set": data}, upsert=True)
            )

        if update_requests:
            collection.bulk_write(update_requests)

    except ConnectionFailure as e:
        print(f"Failed to connect to MongoDB: {e}")

    except CollectionInvalid as e:
        print(f"Failed to create collection: {e}")


# Write the data to MongoDB
df.writeStream \
    .foreachBatch(save_to_mongodb) \
    .outputMode("append") \
    .option("checkpointLocation", "./checkpoint/data") \
    .start() \
    .awaitTermination()

   