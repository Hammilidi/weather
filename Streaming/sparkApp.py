from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder \
    .appName("movielensApp") \
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



#-----------------------------------------CASSANDRA----------------------
from cassandra.cluster import Cluster

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
            lon FLOAT,
            lat FLOAT,
            weather TEXT,
            base TEXT,
            temp FLOAT,
            feels_like FLOAT,
            temp_min FLOAT,
            temp_max FLOAT,
            pressure INT,
            humidity INT,
            visibility INT,
            wind_speed FLOAT,
            wind_deg INT,
            clouds INT,
            dt INT,
            type INT,
            city_id INT,
            country TEXT,
            sunrise INT,
            sunset INT,
            timezone INT,
            city_name TEXT,
            cod INT,
            PRIMARY KEY (city_id) 
        )
        """

        session.execute(create_table_query)
        print(f"Table {keyspaceName}.{tableName} was created successfully.")
    except Exception as e:
        print(f"Error in creating table {keyspaceName}.{tableName}: {str(e)}")

# Establish Cassandra connection
session = connect_to_cassandra(cassandra_host, cassandra_port)

if session:
    create_cassandra_keyspace(session, keyspaceName)

    # Set the keyspace
    session.set_keyspace(keyspaceName)

    create_cassandra_table(session, tableName)
    
    # # Save the DataFrame to Cassandra
    result_df_clean = df.filter(col("id").isNotNull())
    
    # Écrire les données en continu dans Cassandra
    streaming_query = df.writeStream \
        .format("org.apache.spark.sql.cassandra") \
        .outputMode("append") \
        .option("checkpointLocation", "./checkpoint/data") \
        .option("keyspace", keyspaceName) \
        .option("table", tableName) \
        .start()

    # Attendre la terminaison du flux
    streaming_query.awaitTermination()
else:
    print("Exiting due to Cassandra connection failure.")
