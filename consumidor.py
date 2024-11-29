from confluent_kafka import Consumer, KafkaException
import json
from pymongo import MongoClient


client = MongoClient('mongodb://root:example@localhost:27017/')
db = client['pipeline_db']
weather_collection = db['weather_data']
pokemon_collection = db['pokemon_data']


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
        msg = consumer.poll(1.0)  
        if msg is None:
            continue
        if msg.error():
            raise KafkaException(msg.error())
        
        data = json.loads(msg.value().decode('utf-8'))
        
        if msg.topic() == 'TopicA': 
            weather_collection.insert_one(data)
            print(f"Datos del clima almacenados: {data}")
        elif msg.topic() == 'TopicB':  
            try:
                pokemon_collection.insert_one(data)
                print(f"Datos de Pokémon almacenados: {data.get('name', 'Desconocido')}")
            except Exception as e:
                print(f"Error al almacenar datos de Pokémon: {e}")


except Exception as e:
    print(f"Error al consumir mensajes o almacenar en MongoDB: {e}")

finally:
    consumer.close()
