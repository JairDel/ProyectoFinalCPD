import matplotlib.pyplot as plt
import pandas as pd
from pymongo import MongoClient


client = MongoClient('mongodb://root:example@localhost:27017/')
db = client['pipeline_db']
weather_collection = db['weather_data']
pokemon_collection = db['pokemon_data']


weather_data = pd.DataFrame(list(weather_collection.find()))
pokemon_data = pd.DataFrame(list(pokemon_collection.find()))

if 'hourly' in weather_data.columns:
    weather_df = pd.DataFrame(weather_data['hourly'][0])
    weather_df['time'] = pd.to_datetime(weather_df['time'])

    plt.figure(figsize=(10, 5))
    plt.plot(weather_df['time'], weather_df['temperature_2m'], label='Temperatura (°C)')
    plt.title('Temperatura por Hora')
    plt.xlabel('Hora')
    plt.ylabel('Temperatura (°C)')
    plt.grid(True)
    plt.legend()
    plt.show()

if not pokemon_data.empty:
    pokemon_data['name'] = pokemon_data['name'].apply(lambda x: x.capitalize())
    pokemon_data.plot.bar(x='name', y='weight', figsize=(10, 5), color='orange')
    plt.title('Peso de los Pokémon')
    plt.xlabel('Nombre del Pokémon')
    plt.ylabel('Peso')
    plt.grid(axis='y')
    plt.show()
else:
    print("No hay datos de Pokémon disponibles.")
