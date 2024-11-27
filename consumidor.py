from kafka import KafkaConsumer
import json
from pymongo import MongoClient

# Configuración de MongoDB
client = MongoClient('mongodb://localhost:27017/')
db = client['pipeline_db']
weather_collection = db['weather_data']
crypto_collection = db['crypto_data']

# Configuración del consumidor de Kafka
consumer_weather = KafkaConsumer(
    'TopicA',
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

consumer_crypto = KafkaConsumer(
    'TopicB',
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# Consumir los mensajes y almacenar en MongoDB
for message in consumer_weather:
    weather_data = message.value
    weather_collection.insert_one(weather_data)
    print(f"Datos del clima almacenados: {weather_data}")

for message in consumer_crypto:
    crypto_data = message.value
    crypto_collection.insert_one(crypto_data)
    print(f"Datos de criptomonedas almacenados: {crypto_data}")
