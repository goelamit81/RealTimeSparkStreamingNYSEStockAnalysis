import requests
import json
import time

def jsonDefault(object):
    return object.__dict__

def createStockPrice(symbol, jsonStockData):
        for timeInstant,pData in jsonStockData.items():
                p1 = PriceData(pData[OPEN], pData[HIGH], pData[LOW], pData[CLOSE],
                        pData[VOLUME])
                s = StockPrice(symbol,timeInstant,p1);
                return s

class PriceData:
        def __init__(self, open, high, low, close, volume):
                self.open = open
                self.high = high
                self.low = low
                self.close = close
                self.volume = volume

class StockPrice:
	def __init__(self, symbol, timestamp, priceData):
		self.symbol = symbol
		self.timestamp = timestamp
		self.priceData = priceData

stockQueryRequest = "http://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol=%s&interval=1min&outputsize=compact&apikey=HA7C5VX1YEBNZ93E"

var = 1
OPEN = "1. open"
HIGH = "2. high"
LOW = "3. low"
CLOSE = "4. close"
VOLUME = "5. volume"

def getStockPrice(symbol):
	url = stockQueryRequest % (symbol)
	rMsft = requests.get(url, stream=True)
	stockData = rMsft.json()
	print("Fetched data for: "+symbol)
	for key, value in stockData.items():
		if key.startswith('Time'):
			stockData = json.dumps(rMsft.json()[key])
			jsonStockData = json.loads(stockData)
			return createStockPrice(symbol, jsonStockData);

while var == 1 :  # This constructs an infinite loop
	print("Fetching stocks data... ")
	print("=================================")
	stockList = list();
	stockSymbolList = ['MSFT', 'ADBE', 'GOOGL', 'FB'];
	for symbol in stockSymbolList:
		stockPriceObject = getStockPrice(symbol)
		if stockPriceObject is not None:
			stockList.append(stockPriceObject)
	fileName = "C:/temp/sparkinput/stocks_price_"+str(time.time())+".json"
	with open(fileName, 'w') as f:
		print("Writing data to file:"+fileName)
		f.write(json.dumps(stockList, default=jsonDefault))
	time.sleep(50)
	print("=================================")
