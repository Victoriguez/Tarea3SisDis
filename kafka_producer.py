from kafka import KafkaProducer
import json

# Configuración del productor Kafka
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',  # Cambia esto si Kafka está en otro host/puerto
    value_serializer=lambda v: json.dumps(v).encode('utf-8')  # Serialización JSON
)

# Función para enviar incidentes a Kafka
def send_to_kafka(data):
    topic = 'waze-incidents'  # Nombre del tópico en Kafka
    producer.send(topic, data)
    print(f"Enviado a Kafka: {data}")

# Ejemplo de uso: Enviar un incidente de prueba
if __name__ == "__main__":
    # Simulación de un incidente recopilado por el scraper
    incident_example = {
        'type': 'Accidente',
        'location': 'Av. Principal y Calle Falsa',
        'severity': 'Alta',
        'time_reported': '2024-02-20 14:30:00'
    }
    
    # Enviar el incidente a Kafka
    send_to_kafka(incident_example)
    
    # Asegurarse de que los mensajes se envían antes de salir
    producer.flush()
