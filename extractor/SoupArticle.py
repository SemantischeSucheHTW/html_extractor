import datetime
from bs4 import BeautifulSoup
import re

class SoupArticle:
 
    def __init__(self, html):
        self.soup=BeautifulSoup(html, 'html.parser')
        
        try:
            self.article = self.soup.find_all("div", class_="column-content")[0]
        except:
            print("Error while extracting article section from html")
    
    def getTitle(self):
        title = None
        try:
            title = self.article.find("h1", class_="title").text
        except:
            print("Error while extracting title from article")
        return title
            
    def getLocation(self):
        location = None
        try:
            location = self.article.find_all("div", class_="polizeimeldung")[1].get_text()
            location = location.replace(" ", "")
        except:
            print("Error while extracting location from article")
        return location
            
    def getDate(self):
        date = None
        try:
            dateStringRaw = self.article.find_all("div", class_="polizeimeldung")[0].get_text()
            #date = re.search("\d{3}", dateString).group(1)
            dateStringCropped = re.findall("[0-9]+[.][0-9]+[.][0-9]+", dateStringRaw)[0]
            split_date = dateStringCropped.split(".")
            day = int(split_date[0])
            month = int(split_date[1])
            year = int(split_date[2])

            date=datetime.date(year, month, day)
        except:
            print("Error while extracting date from article")
        return date
    
    def getNr(self):
        nr = None
        #nr = -1
        try:
            """nrString = self.article.find("strong").text
            nr=int(re.findall("[0-9]+", nrString)[0])
            #nr=re.findall("[0-9]+", nrString)[0]"""
            
            #looking for something like Nr. 5542 in the raw html article
            nrString = re.findall("Nr\. [0-9]+", self.article.prettify())[0]
            nr = re.findall("[0-9]+", nrString)[0]
        except:
            print("Error while extracting nr from article")
        return nr
    
    def getText(self):
        text = None
        try:
            text = self.article.find("div", class_="textile")
            [x.extract() for x in text.findAll('strong')] #if the strong tag occurrs, it gets sorted out #source:Tilman
            text = text.text.replace('\n', '').replace("  ", "")
            #text.strong.extract() #<- if this fails, the article text is extracted anyways! The number is put at the beginning too though
        except:
            print("Error while extracting text from article")
        return text