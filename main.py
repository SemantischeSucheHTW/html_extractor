import os

from HTMLExtractor import HTMLExtractor
from kafkasink import KafkaSink
from kafkasource import KafkaSource
from mongodbdao import MongoDBDao


def env(key):
    value = os.environ.get(key)
    if not value:
        raise Exception(f"environment variable '{key}' not set!")
    return value

debug = env("DEBUG")

orderSource = KafkaSource({
    "topic": env("KAFKA_PARSEORDERS_TOPIC"),
    "bootstrap_servers": env("KAFKA_BOOTSTRAP_SERVERS"),
    "group_id": env("KAFKA_PARSEORDERS_GROUP_ID"),
    "auto_offset_reset": "earliest"
})

dao = MongoDBDao({
    "host": env("MONGODB_HOST"),
    "db": env("MONGODB_DB"),
    "rawpages_collection": env("MONGODB_RAWPAGES_COLLECTION"),
    "pagedetails_collection": env("MONGODB_PAGEDETAILS_COLLECTION"),
    "username": env("MONGODB_USERNAME"),
    "password": env("MONGODB_PASSWORD"),
    "authSource": env("MONGODB_DB")
})

urlsink = KafkaSink({ # to notify downstream actors
    "topic": env("KAFKA_PAGEDETAILS_TOPIC"),
    "bootstrap_servers": env("KAFKA_BOOTSTRAP_SERVERS")
})

extractor = HTMLExtractor(dao)

while True:
    order = orderSource.getOrder()

    if debug:
        print(f"Got order {order}")
    stored_details = extractor.process(order)
    if debug:
        print(f"Stored details for {stored_details}")
    urlsink.send(order.url)
