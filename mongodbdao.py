from pymongo import MongoClient

from RawPageData import RawPageData

import datetime

class MongoDBDao():

    def __init__(self, config):

        conf_copy = dict(config)
        db = conf_copy.pop("db")
        rawpages_collection_name = conf_copy.pop("rawpages_collection")
        pagedetails_collection_name = conf_copy.pop("pagedetails_collection")

        self.client = MongoClient(**conf_copy)
        self.db = self.client[db]
        self.rawpages_collection = self.db[rawpages_collection_name]
        self.pagedetails_collection = self.db[pagedetails_collection_name]

    def getRawPageData(self, order):

        doc = self.rawpages_collection.find_one(f"{order.url};{order.timestamp.isoformat()}")

        if (not doc):
            raise Exception("Did not find RawPageData with the given ParseOrder")

        return RawPageData(doc["url"],
                           datetime.datetime.fromisoformat(doc["datetime"]),
                           doc["statuscode"],
                           doc["header"],
                           doc["body"])

    def storePageDetails(self, details):

        doc = {
            "_id": details.url,
            "url": details.url,
            "title": details.title,
            "location": details.location,
            "date": details.date.isoformat(),
            "nr": details.nr,
            "text": details.text
        }

        self.pagedetails_collection.find_one_and_replace({"_id": details.url}, doc, upsert=True)