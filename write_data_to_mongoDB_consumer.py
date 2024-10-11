import pika
import json
from pymongo import MongoClient

# Соединение с MongoDB
mongo_client = MongoClient("mongodb://localhost:27017/")
db = mongo_client["binance_data"]
collection = db["market_data"]

# Функция для записи данных в MongoDB
def save_to_mongo(data):
    try:
        collection.insert_many(data)
        print("Данные успешно сохранены в MongoDB")
    except Exception as e:
        print(f"Ошибка записи в MongoDB: {e}")

# Соединение с RabbitMQ
connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()
channel.queue_declare(queue='binance_data_queue')

# Функция для получения данных из RabbitMQ
def callback(ch, method, properties, body):
    data = json.loads(body)
    save_to_mongo(data)

# Настройка консьюмера
channel.basic_consume(queue='binance_data_queue', on_message_callback=callback, auto_ack=True)

print('Ожидание сообщений. Нажмите Ctrl+C для выхода.')
channel.start_consuming()
