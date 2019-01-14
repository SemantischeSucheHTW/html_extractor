import datetime

class Order:

    def __init__(self, url: str, timestamp: datetime.datetime):
        assert isinstance(timestamp, datetime.datetime)

        self.url = url
        self.timestamp = timestamp

    def __str__(self):
        return f"{self.url};{self.timestamp}"