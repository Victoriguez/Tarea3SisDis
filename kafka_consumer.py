from kafka import KafkaConsumer
import json

# Configuración del consumidor Kafka
consumer = KafkaConsumer(
    'waze-incidents',  # Nombre del tópico
    bootstrap_servers='localhost:9092',  # Cambia esto si Kafka está en otro host/puerto
    auto_offset_reset='earliest',  # Leer desde el principio del tópico
    enable_auto_commit=True,
    group_id='incident-consumer-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))  # Deserialización JSON
)

# Función para procesar los mensajes recibidos
def consume_messages():
    print("Iniciando consumidor de Kafka...")
    for message in consumer:
        incident = message.value  # Mensaje deserializado
        print(f"Incidente recibido: {incident}")
        
        # Aquí puedes agregar lógica de procesamiento adicional o almacenamiento en bases de datos

# Ejecutar el consumidor
if __name__ == "__main__":
    consume_messages()
