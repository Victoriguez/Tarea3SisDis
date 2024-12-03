import requests
from bs4 import BeautifulSoup
from kafka import KafkaProducer
import json
import time

# Configurar el productor Kafka
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',  # Cambia esto si tu servidor Kafka está en otra dirección
    value_serializer=lambda v: json.dumps(v).encode('utf-8')  # Serializar los datos a JSON
)

def scrape_waze_data():
    url = "https://www.waze.com/live-map/"  # URL base de Waze (ajustar según la fuente real)
    
    try:
        # Realizar una solicitud HTTP
        response = requests.get(url)
        if response.status_code != 200:
            print(f"Error al acceder a Waze: {response.status_code}")
            return []

        # Analizar el contenido HTML de la página
        soup = BeautifulSoup(response.content, 'html.parser')

        # Extraer la información específica de incidentes (estructura simulada)
        incidents = []
        for incident in soup.find_all('div', class_='incident-class'):  # Ajustar según la estructura real
            incident_type = incident.find('span', class_='type').text
            location = incident.find('span', class_='location').text
            severity = incident.find('span', class_='severity').text
            time_reported = incident.find('span', class_='time').text

            # Guardar el incidente en un diccionario
            incident_data = {
                'type': incident_type,
                'location': location,
                'severity': severity,
                'time_reported': time_reported
            }

            # Enviar el incidente a Kafka
            producer.send('waze-incidents', incident_data)
            print(f"Enviado a Kafka: {incident_data}")

        return incidents

    except Exception as e:
        print(f"Error durante el scraping: {e}")

# Ejecutar el scraper en intervalos regulares
if __name__ == "__main__":
    while True:
        scrape_waze_data()
        time.sleep(60)  # Ejecutar cada 60 segundos (ajustable)
