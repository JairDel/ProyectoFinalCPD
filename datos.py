import matplotlib.pyplot as plt
import pandas as pd

# Obtener los datos desde MongoDB
weather_data = pd.DataFrame(list(weather_collection.find()))
crypto_data = pd.DataFrame(list(crypto_collection.find()))

# Graficar los datos de temperatura
plt.figure(figsize=(10, 5))
plt.plot(weather_data['hourly']['temperature_2m'], label='Temperatura (°C)')
plt.title('Temperatura por hora')
plt.xlabel('Hora')
plt.ylabel('Temperatura (°C)')
plt.legend()
plt.show()

# Graficar los datos de criptomonedas
plt.figure(figsize=(10, 5))
plt.plot(crypto_data['bitcoin']['usd'], label='Bitcoin (USD)')
plt.plot(crypto_data['ethereum']['usd'], label='Ethereum (USD)')
plt.title('Precios de Criptomonedas')
plt.xlabel('Hora')
plt.ylabel('Precio (USD)')
plt.legend()
plt.show()
