from extractor.PageDetails import *
from extractor.SoupArticle import *
import datetime
import re

class HTMLExtractor:

    '''
    Constructor
    :param dao: data access object from which RawPageData-Objects can be retrieved and PageDetail-Objects stored to
    '''
    def __init__(self, dao):
        self.dao=dao
    
    def process(self, order):
        rawpage=self.dao.getRawPageData(order)
        
        url=order.url
        
        soup_article = SoupArticle(rawpage.body)
        
        title = soup_article.getTitle()
        location = soup_article.getLocation()
        date= soup_article.getDate()
        nr = soup_article.getNr()
        text = soup_article.getText()
        
        details = PageDetails(url, title, location, date, nr, text)
        
        self.dao.storePageDetails(details)
        return details