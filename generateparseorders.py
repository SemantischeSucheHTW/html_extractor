import json
from datetime import datetime

import math
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

cursor = rawpages_collection.aggregate([
    {
        "$lookup": {
            "from": env("MONGODB_PAGEDETAILS_COLLECTION"),
            "localField": "url",
            "foreignField": "url",
            "as": "matched_docs"
        }
    },
    {
        "$match": {
            #"matched_docs": {"$not": {"$size": 0}},
            "matched_docs": [],
        }
    }
])

number = 1
for rawpage in cursor:
    kafka_producer.send(
        env("KAFKA_PARSEORDERS_TOPIC"),
        key=rawpage["url"],
        value=Order(rawpage["url"], datetime.fromisoformat(rawpage["datetime"]))
    )

    number = number + 1

    threshold = 10 ** math.floor(math.log10(number)) * 5

    if number % threshold == 0:
        print(f"Sent document no. {number}, key: {rawpage['url']};{rawpage['datetime']}")

print(f"Sent {number} documents.")
