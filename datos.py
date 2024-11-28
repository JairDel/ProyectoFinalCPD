import matplotlib.pyplot as plt
import pandas as pd
from pymongo import MongoClient

# Conectar con MongoDB
client = MongoClient('mongodb://root:example@localhost:27017/')
db = client['pipeline_db']
weather_collection = db['weather_data']
crypto_collection = db['crypto_data']

# Obtener los datos de la colección y manejar datos anidados
weather_data = pd.DataFrame(list(weather_collection.find()))
crypto_data = pd.DataFrame(list(crypto_collection.find()))

# Extraer y convertir datos anidados de clima
if 'hourly' in weather_data.columns:
    weather_df = pd.DataFrame(weather_data['hourly'][0])  # Desanidar el primer documento
    weather_df['time'] = pd.to_datetime(weather_df['time'])
else:
    print("La colección de clima no tiene datos esperados.")

# Graficar datos de temperatura
plt.figure(figsize=(10, 5))
plt.plot(weather_df['time'], weather_df['temperature_2m'], label='Temperatura (°C)')
plt.title('Temperatura por Hora')
plt.xlabel('Hora')
plt.ylabel('Temperatura (°C)')
plt.grid(True)
plt.legend()
plt.show()

# Extraer y convertir datos anidados de criptomonedas
if 'bitcoin' in crypto_data.columns and 'ethereum' in crypto_data.columns:
    crypto_df = pd.DataFrame(crypto_data[['bitcoin', 'ethereum']].applymap(lambda x: x['usd']))
    crypto_df['time'] = pd.to_datetime(crypto_data['_id'].apply(lambda x: x.generation_time))  # O puedes tener un campo explícito para timestamps
else:
    print("La colección de criptomonedas no tiene datos esperados.")

# Graficar datos de criptomonedas
plt.figure(figsize=(10, 5))
plt.plot(crypto_df['time'], crypto_df['bitcoin'], label='Bitcoin (USD)')
plt.plot(crypto_df['time'], crypto_df['ethereum'], label='Ethereum (USD)')
plt.title('Precios de Criptomonedas')
plt.xlabel('Hora')
plt.ylabel('Precio (USD)')
plt.grid(True)
plt.legend()
plt.show()
