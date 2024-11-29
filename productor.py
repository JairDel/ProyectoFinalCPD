import requests
import json
import time
from confluent_kafka import Producer
import random

producer = Producer({'bootstrap.servers': 'localhost:9092'})

def delivery_report(err, msg):
    if err is not None:
        print(f'Error al entregar mensaje: {err}')
    else:
        print(f'Mensaje entregado a {msg.topic()} [{msg.partition()}]')


def fetch_weather_data():
    url = 'https://api.open-meteo.com/v1/forecast?latitude=35&longitude=139&hourly=temperature_2m'
    response = requests.get(url)
    return response.json()


def fetch_pokemon_data():
    pokemon_id = random.randint(1, 898)  
    url = f'https://pokeapi.co/api/v2/pokemon/{pokemon_id}'
    response = requests.get(url)
    return response.json()

while True:
    weather_data = fetch_weather_data()
    pokemon_data = fetch_pokemon_data()

    producer.produce('TopicA', key=None, value=json.dumps(weather_data), callback=delivery_report)
    producer.produce('TopicB', key=None, value=json.dumps(pokemon_data), callback=delivery_report)

    producer.flush()
    print('Datos enviados a Kafka')
    time.sleep(60)
