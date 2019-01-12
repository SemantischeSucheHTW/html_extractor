import json
from datetime import datetime

import os
from kafka import KafkaProducer
from pymongo import MongoClient

from Order import Order

def env(key):
    value = os.environ.get(key)
    if not value:
        raise Exception(f"environment variable {key} not set!")
    return value

kafka_producer = KafkaProducer(
    key_serializer = str.encode,
    value_serializer = lambda order: json.dumps({"url": order.url, "datetime": datetime.isoformat(order.timestamp)}).encode('utf-8'),
    bootstrap_servers = env("KAFKA_BOOTSTRAP_SERVERS")
)

mongo_client = MongoClient(
    host = env("MONGODB_HOST"),
    username = env("MONGODB_USERNAME"),
    password = env("MONGODB_PASSWORD"),
    authSource = env("MONGODB_DB")
)

rawpages_collection = mongo_client[env("MONGODB_DB")][env("MONGODB_RAWPAGES_COLLECTION")]

doc_no = 1

for rawpage in rawpages_collection.find():
    kafka_producer.send(
        env("KAFKA_PARSEORDERS_TOPIC"),
        key=rawpage["url"],
        value=Order(rawpage["url"], datetime.fromisoformat(rawpage["datetime"]))
    )
    print(f"\rSent document no. {doc_no}, key: {rawpage['url']};{rawpage['datetime']}", end="")
    doc_no = doc_no + 1