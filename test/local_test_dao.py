from dao.RawPageData import RawPageData

class LocalTestDao:
 
    def getRawPageData(self, order):
        #file = open("exp_newest.html", "r")
        #file = open("exp_normal_alt.html", "r")
        file = open(order.url, "r")
        text = ""
        for line in file:
            text+=line
        
        rpd = RawPageData("url", None, 200, None, text)
        return rpd
    
    def storePageDetails(self, details):
        print("URL:", details.url)
        print("Title:", details.title)
        print("Location:", details.location)
        print("Date:", details.date)
        print("Text:", details.text)
        print("Nr:", details.nr)
        print("\n")