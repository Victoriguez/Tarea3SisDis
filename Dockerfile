# Usar la imagen base de Python
FROM python:3.9

# Instalar Java (requerido por Spark)
RUN apt-get update && apt-get install -y openjdk-11-jdk && \
    apt-get clean

# Configurar variables de entorno para Java
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH=$PATH:$JAVA_HOME/bin

# Instalar dependencias del proyecto
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copiar el código del proyecto en el contenedor
COPY . /app
WORKDIR /app

# Comando para ejecutar el script de Spark
CMD ["python", "spark_kafka_consumer.py"]
