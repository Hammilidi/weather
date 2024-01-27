from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, ArrayType, IntegerType, MapType, FloatType
from pyspark.sql.functions import from_json, col, expr
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, CollectionInvalid



# Initialize Spark session
spark = SparkSession.builder \
    .appName("weatherElectricity") \
    .config("spark.streaming.stopGracefullyOnShutdown", True) \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,com.datastax.spark:spark-cassandra-connector_2.12:3.2.0") \
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
    col("coord.lon").alias("longitude"),
    col("coord.lat").alias("latitude"),
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
    col("timestamp").alias("electricity_timestamp"),
    col("utilisateur.comportement").alias("user_behavior"),
    col("utilisateur.type_utilisateurs").alias("user_type"),
    col("zoneKey").alias("zone_key")
)


# # Write the data to console
# streaming_query = df.writeStream \
#     .format("console") \
#     .outputMode("append") \
#     .option("checkpointLocation", "./checkpoint/data") \
#     .start()

# streaming_query.awaitTermination()  # Wait for the processing to finish


#--------------------------------------------------MongoDB---------------------------------------------------------------


def save_to_mongodb(iter):
    try:
        client = MongoClient("mongodb://localhost:27017")
        db_name = "weather"
        collection_name = "weather_data"

        db = client[db_name]
        collection = db[collection_name]

        for row in iter:
            data = {
                "longitude": row.longitude,
                "latitude": row.latitude,
                "weather": row.weather,
                "base": row.base,
                "temperature": row.temperature,
                "feels_like": row.feels_like,
                "min_temperature": row.min_temperature,
                "max_temperature": row.max_temperature,
                "pressure": row.pressure,
                "humidity": row.humidity,
                "visibility": row.visibility,
                "wind_speed": row.wind_speed,
                "wind_degree": row.wind_degree,
                "cloudiness": row.cloudiness,
                "datetime": row.datetime,
                "sys_type": row.sys_type,
                "sys_id": row.sys_id,
                "country": row.country,
                "sunrise": row.sunrise,
                "sunset": row.sunset,
                "timezone": row.timezone,
                "city_id": row.city_id,
                "city_name": row.city_name,
                "cod": row.cod,
                "electricity_units": row.electricity_units,
                "electricity_value": row.electricity_value,
                "electricity_country": row.electricity_country,
                "electricity_production": row.electricity_production,
                "electricity_timestamp": row.electricity_timestamp,
                "user_behavior": row.user_behavior,
                "user_type": row.user_type,
                "zone_key": row.zone_key
            }

            collection.insert_one(data)

    except ConnectionFailure as e:
        print(f"Failed to connect to MongoDB: {e}")

    except CollectionInvalid as e:
        print(f"Failed to create collection: {e}")

# Write the data to MongoDB
df.writeStream \
    .foreachBatch(save_to_mongodb) \
    .outputMode("append") \
    .start() \
    .awaitTermination()

