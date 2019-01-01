class PageDetails:

    def __init__(self, url, title, location, date, nr, text):
        #URL of downloaded HTML-File: String
        self.url = url
        
        #title of the article: String
        self.title = title
        
        #location of the incident: String
        self.location = location
        
        #date of the incident: datetime.date
        self.date = date

        #number of the article: Integer -> maybe as String later? Note that the number can easily be None, so don't rely on it
        self.nr = nr

        #text of the article: String
        self.text = text