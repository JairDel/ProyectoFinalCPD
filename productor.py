from confluent_kafka import Producer
import requests
import json
import time

# Configuración del productor
producer = Producer({'bootstrap.servers': 'localhost:9092'})

def delivery_report(err, msg):
    if err is not None:
        print('Error al entregar mensaje: {}'.format(err))
    else:
        print('Mensaje entregado a {} [{}]'.format(msg.topic(), msg.partition()))

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

    producer.produce('TopicA', key=None, value=json.dumps(weather_data), callback=delivery_report)
    producer.produce('TopicB', key=None, value=json.dumps(crypto_data), callback=delivery_report)

    producer.flush()
    print('Datos enviados a Kafka')
    time.sleep(60)
