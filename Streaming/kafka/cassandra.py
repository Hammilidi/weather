from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
from pyspark.sql.functions import from_json, col

# Initialize Spark session
spark = SparkSession.builder \
    .appName("weatherCassandra") \
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

# -----------------------------------------CASSANDRA----------------------
from cassandra.cluster import Cluster
from pyspark.sql import DataFrame


# parametres de connexion à cassandra
cassandra_host = 'localhost'
cassandra_port = 9042
keyspaceName = 'weather'
tableName = 'weather_data'

def connect_to_cassandra(host, port):
    try:
        # Provide contact points
        cluster = Cluster([host], port=port)
        session = cluster.connect()
        print("Connection established successfully.")
        return session
    except Exception as e:
        print("Connection failed: ", str(e))
        return None

def create_cassandra_keyspace(session, keyspaceName):
    try:
        create_keyspace_query = f"""
            CREATE KEYSPACE IF NOT EXISTS {keyspaceName}
            WITH REPLICATION = {{'class': 'SimpleStrategy', 'replication_factor': 1}}
        """
        session.execute(create_keyspace_query)
        print(f"Keyspace {keyspaceName} was created successfully.")
    except Exception as e:
        print(f"Error in creating keyspace {keyspaceName}: {str(e)}")

def create_cassandra_table(session, tableName):
    try:
        create_table_query = f"""
            CREATE TABLE IF NOT EXISTS {keyspaceName}.{tableName} (
                longitude FLOAT,
                latitude FLOAT,
                weather TEXT,
                base TEXT,
                temperature FLOAT,
                feels_like FLOAT,
                min_temperature FLOAT,
                max_temperature FLOAT,
                pressure INT,
                humidity INT,
                visibility INT,
                wind_speed FLOAT,
                wind_degree INT,
                cloudiness INT,
                datetime INT,
                sys_type INT,
                sys_id INT,
                country TEXT,
                sunrise INT,
                sunset INT,
                timezone INT,
                city_id INT,
                city_name TEXT,
                cod INT,
                PRIMARY KEY ((city_id, datetime))
            )
        """
        session.execute(create_table_query)
        print(f"Table {keyspaceName}.{tableName} was created successfully.")
    except Exception as e:
        print(f"Error in creating table {keyspaceName}.{tableName}: {str(e)}")

def save_to_cassandra(batch_df, batch_id):
    try:
        batch_df.coalesce(1).write \
            .format("org.apache.spark.sql.cassandra") \
            .mode("append") \
            .option("keyspace", keyspaceName) \
            .option("table", tableName) \
            .save()
        print(f"Batch {batch_id} inserted into Cassandra successfully.")
    except Exception as e:
        print(f"Error while inserting batch {batch_id} into Cassandra: {str(e)}")

# Establish Cassandra connection
session = connect_to_cassandra(cassandra_host, cassandra_port)

if session:
    create_cassandra_keyspace(session, keyspaceName)
    session.set_keyspace(keyspaceName)
    create_cassandra_table(session, tableName)

    result_df_clean = df.filter(col("city_id").isNotNull())

    # Insérer dans Cassandra
    df.writeStream \
        .foreachBatch(save_to_cassandra) \
        .outputMode("append") \
        .start() \
        .awaitTermination()
else:
    print("Exiting due to Cassandra connection failure.")
    

