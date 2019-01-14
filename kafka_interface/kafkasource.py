from kafka import KafkaConsumer
from kafka_interface.Order import Order

import json
from datetime import datetime

class KafkaSource:

    @staticmethod
    def _parseRawPageData(bytes):
        base = json.loads(bytes.decode("utf-8"))
        base["timestamp"] = datetime.fromisoformat(base.pop("datetime"))
        return Order(**base)

    def __init__(self, config):

        config["key_deserializer"] = lambda k: k.decode("utf-8")
        config["value_deserializer"] = KafkaSource._parseRawPageData
        topic = config.pop("topic")

        self.consumer = KafkaConsumer(topic, **config)

    def getOrder(self):
        kv = next(self.consumer)
        assert isinstance(kv.value, Order)
        return kv.value