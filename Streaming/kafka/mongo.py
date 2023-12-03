from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.functions import col, expr
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, CollectionInvalid




# Initialize Spark session
spark = SparkSession.builder \
    .appName("weatherMongodb") \
    .config("spark.streaming.stopGracefullyOnShutdown", True) \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,com.datastax.spark:spark-cassandra-connector_2.12:3.2.0") \
    .getOrCreate()

# Read from Kafka
kafkaStream = spark.readStream \
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

# Parse Kafka messages and apply schema
parsed_stream = kafkaStream.selectExpr("CAST(value AS STRING)")
df = parsed_stream.withColumn("values", from_json(parsed_stream["value"], weather_schema))

# Access fields within the struct
df = df.select("values.*")

# Transformations
# Renommer la colonne sys.id en sys_id
df = df.select(
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
    col("cod").alias("cod")
)


def save_to_mongodb(iter):
    try:
        client = MongoClient("mongodb://localhost:27017")
        db_name = "weather"
        collection_name = "weather_data"

        db = client[db_name]
        collection = db[collection_name]

        data = {
            "longitude": iter.longitude,
            "latitude": iter.latitude,
            "weather": iter.weather,
            "base": iter.base,
            "temperature": iter.temperature,
            "feels_like": iter.feels_like,
            "min_temperatuer": iter.min_temperature,
            "max_temperature": iter.max_temperature,
            "pressure": iter.pressure,
            "humidity": iter.humidity,
            "visibility": iter.visibility,
            "wind_speed": iter.wind_speed,
            "wind_degree": iter.wind_degree,
            "cloudiness": iter.cloudiness,
            "datetime": iter.datetime,
            "sys_type": iter.sys_type,
            "sys_id": iter.sys_id,
            "country": iter.country,
            "sunrise": iter.sunset,
            "sunset": iter.sunset,
            "timezone": iter.timezone,
            "city_id": iter.city_id,
            "city_name": iter.city_name,
            "cod": iter.cod
        }

        collection.insert_one(data)

    except ConnectionFailure as e:
        print(f"Failed to connect to MongoDB: {e}")

    except CollectionInvalid as e:
        print(f"Failed to create collection: {e}")



# Write the data to MongoDB
df.writeStream \
    .foreachBatch(lambda batch_df, batch_id: batch_df.coalesce(1).write.format("json").save(f"output/{batch_id}")) \
    .foreach(save_to_mongodb) \
    .outputMode("append") \
    .start() \
    .awaitTermination()
