from pymongo import MongoClient

# Соединение с MongoDB
client = MongoClient("mongodb://localhost:27017/")
db = client["binance_data"]
collection = db["market_data"]

# Выбор всех данных из коллекции
data = collection.find()

# Вывод данных
for document in data:
    print(document)
