from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, TimestampType

# Crear la sesión de Spark
spark = SparkSession.builder \
    .appName("WazeTrafficStreaming") \
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

# Procesar los datos (ejemplo: filtrar incidentes graves)
filtered_df = json_df.filter(json_df["severity"] == "Alta")

# Mostrar los resultados en la consola (para pruebas)
query = filtered_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

# Mantener la consulta activa
query.awaitTermination()
