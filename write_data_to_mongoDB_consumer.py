# write_data_to_mongoDB_consumer
import pika
import json
from pymongo import MongoClient

# Соединение с MongoDB
mongo_client = MongoClient("mongodb://localhost:27017/")
db = mongo_client["binance_data"]

# Функция для записи данных в MongoDB в зависимости от интервала
def save_to_mongo(data, interval):
    collection = db[f"market_data_{interval}"]
    try:
        collection.insert_many(data)
        print(f"Данные для интервала {interval} успешно сохранены в MongoDB")
    except Exception as e:
        print(f"Ошибка записи данных для интервала {interval} в MongoDB: {e}")

# Соединение с RabbitMQ
connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()
channel.queue_declare(queue='binance_data_queue')

# Функция для получения данных из RabbitMQ
def callback(ch, method, properties, body):
    message = json.loads(body)
    data = message['data']
    interval = message['interval']  # Получаем интервал из сообщения
    save_to_mongo(data, interval)

# Настройка консьюмера
channel.basic_consume(queue='binance_data_queue', on_message_callback=callback, auto_ack=True)

print('Ожидание сообщений. Нажмите Ctrl+C для выхода.')
channel.start_consuming()