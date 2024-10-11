import pika
from binance.client import Client
import pandas as pd
import json
from pymongo import MongoClient


# Загрузка конфигурации
with open('api_credentials.json', 'r') as config_file:
    config = json.load(config_file)

api_key = config['api_key']
api_secret = config['api_secret']

# Соединение с Binance API
client = Client(api_key, api_secret)

# Настройка MongoDB
mongo_client = MongoClient("mongodb://localhost:27017/")
db = mongo_client["binance_data"]
collection = db["market_data"]

# Соединение с RabbitMQ
connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()
channel.queue_declare(queue='binance_data_queue')

# Настройки символа
symbol = 'BTCUSDT'

# Разные интервалы
intervals = {
    '5m': Client.KLINE_INTERVAL_5MINUTE,
    '1h': Client.KLINE_INTERVAL_1HOUR,
    '4h': Client.KLINE_INTERVAL_4HOUR,
    '1d': Client.KLINE_INTERVAL_1DAY
}

start_date = '2023-01-01'
end_date = '2023-01-21'

# Функция для получения данных с Binance и отправки в RabbitMQ
def fetch_and_send_data(symbol, interval, start_date, end_date):
    klines = client.get_historical_klines(symbol, interval, start_date, end_date)
    df = pd.DataFrame(klines, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume', 'close_time',
                                       'quote_asset_volume', 'number_of_trades', 'taker_buy_base_asset_volume',
                                       'taker_buy_quote_asset_volume', 'ignore'])

    # Преобразование 'timestamp' и 'close_time' в строку, чтобы избежать проблем с сериализацией
    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms').astype(str)
    df['close_time'] = pd.to_datetime(df['close_time'], unit='ms').astype(str)

    # Преобразование данных в JSON-формат
    json_data = df.to_dict(orient='records')

    # Отправка данных в RabbitMQ
    channel.basic_publish(exchange='', routing_key='binance_data_queue', body=json.dumps(json_data))
    print(f"Данные отправлены для интервала {interval}")

# Цикл по всем интервалам для получения данных
for name, interval in intervals.items():
    fetch_and_send_data(symbol, interval, start_date, end_date)

# Закрытие соединения с RabbitMQ
connection.close()