class RawPageData:

    def __init__(self, url, datetime, statuscode, header, body):
        #downloaded URL: String
        self.url=url
        
        #date & time at which the download was performed: import datetime -> datetime.datetime.now()
        self.datetime=datetime

        #transfer: String
        #self.protocol=protocol

        #statuscode: Int
        self.statuscode=statuscode

        #things like content-type, content-length: Dict
        self.header=header

        #the downloaded document itself: String
        self.body=body