from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType
from cassandra.cluster import Cluster
from elasticsearch import Elasticsearch

# Crear la sesión de Spark
spark = SparkSession.builder \
    .appName("WazeTrafficStreaming") \
    .config("spark.cassandra.connection.host", "cassandra") \
    .getOrCreate()

# Definir el esquema de los datos de incidentes
schema = StructType() \
    .add("type", StringType()) \
    .add("location", StringType()) \
    .add("severity", StringType()) \
    .add("time_reported", StringType())

# Configurar el flujo de datos desde Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "waze-incidents") \
    .load()

# Deserializar los datos de JSON
df = df.selectExpr("CAST(value AS STRING)")
json_df = df.select(from_json(col("value"), schema).alias("data")).select("data.*")

# Filtrar los incidentes de severidad alta
filtered_df = json_df.filter(json_df["severity"] == "Alta")

# Enviar los datos a Elasticsearch
def send_to_elasticsearch(row):
    es = Elasticsearch(['http://elasticsearch:9200'])
    es.index(index='waze-incidents', body=row.asDict())

# Enviar los datos a Cassandra
def send_to_cassandra(row):
    cluster = Cluster(['cassandra'])
    session = cluster.connect()

    # Crear keyspace y tabla si no existen
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS traffic WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}
    """)
    session.execute("""
        CREATE TABLE IF NOT EXISTS traffic.incidents (
            type text,
            location text,
            severity text,
            time_reported text,
            PRIMARY KEY (time_reported, location)
        )
    """)

    # Insertar los datos en Cassandra
    query = """
        INSERT INTO traffic.incidents (type, location, severity, time_reported)
        VALUES (%s, %s, %s, %s)
    """
    session.execute(query, (row['type'], row['location'], row['severity'], row['time_reported']))

# Escribir los resultados en ambos sistemas
query = filtered_df.writeStream \
    .foreachBatch(lambda batch_df, _: batch_df.foreach(send_to_elasticsearch)) \
    .foreachBatch(lambda batch_df, _: batch_df.foreach(send_to_cassandra)) \
    .outputMode("append") \
    .start()

# Mantener la consulta activa
query.awaitTermination()
