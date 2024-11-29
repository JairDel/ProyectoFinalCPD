from confluent_kafka import Consumer, KafkaException
import json
from pymongo import MongoClient

# Configuración de MongoDB con autenticación
client = MongoClient('mongodb://root:example@localhost:27017/')
db = client['pipeline_db']
weather_collection = db['weather_data']
crypto_collection = db['crypto_data']

# Configuración del consumidor de Kafka
consumer_config = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'mi_grupo_consumidor',
    'auto.offset.reset': 'earliest'
}

consumer = Consumer(consumer_config)
consumer.subscribe(['TopicA', 'TopicB'])

print("Esperando mensajes de Kafka...")

try:
    while True:
        msg = consumer.poll(1.0)  # Espera 1 segundo por mensajes
        if msg is None:
            continue
        if msg.error():
            raise KafkaException(msg.error())
        
        data = json.loads(msg.value().decode('utf-8'))
        
        if msg.topic() == 'TopicA':  # Datos del clima
            weather_collection.insert_one(data)
            print(f"Datos del clima almacenados: {data}")
        elif msg.topic() == 'TopicB':  # Datos de criptomonedas
            crypto_collection.insert_one(data)
            print(f"Datos de criptomonedas almacenados: {data}")

except Exception as e:
    print(f"Error al consumir mensajes o almacenar en MongoDB: {e}")

finally:
    consumer.close()
