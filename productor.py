import requests
from kafka import KafkaProducer
import json
import time

# Configuración de Kafka
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# API de Open Meteo
def fetch_weather_data():
    url = 'https://api.open-meteo.com/v1/forecast?latitude=35&longitude=139&hourly=temperature_2m'
    response = requests.get(url)
    return response.json()

# API de CoinGecko
def fetch_crypto_data():
    url = 'https://api.coingecko.com/api/v3/simple/price?ids=bitcoin,ethereum&vs_currencies=usd'
    response = requests.get(url)
    return response.json()

# Enviar datos a Kafka
while True:
    weather_data = fetch_weather_data()
    crypto_data = fetch_crypto_data()
    
    producer.send('TopicA', weather_data)  # Enviar datos del clima
    producer.send('TopicB', crypto_data)  # Enviar datos de criptomonedas

    print('Datos enviados a Kafka')
    time.sleep(60)  # Esperar 60 segundos antes de hacer la siguiente solicitud
