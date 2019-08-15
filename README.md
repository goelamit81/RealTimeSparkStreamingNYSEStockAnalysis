# RealTimeSparkStreamingNYSEStockAnalysis

Once the NYSE opens, a script should run to fetch the data relating to the stocks every minute inside a folder. The script will make use of the API provided by Alpha Vantage as described above. In parallel, a Spark application should run to stream data from the folder every minute and then perform the analyses on the data. The results of the analyses should be written in an output file. These results will act as insights to make informed decisions related to the stocks.

Fetch data every minute relating to the following four stocks:

Facebook (FB),
Google (GOOGL),
Microsoft (MSFT),
Adobe (ADBE)

It can be achieved by using the python script that hits the intraday API for each stock every minute. We will provide you with a python script and the steps to run in the resources section ahead. On hitting the API for each stock every minute, the script will get the response corresponding to each stock every minute. From the response received, it will extract the latest minute data and will dump that data for each stock every minute in a new file inside a folder. 

Note: The above stocks are listed on NYSE (New York Stock Exchange) located in EDT timezone. Daily opening hours for NYSE are from 09:30 AM to 04:00 PM GMT(-4) and it remains closed on Saturday and Sunday. While running the script, keep note of the timezone and the opening hours to get the data. Run the script during the time NYSE remains open. Once the exchange closes, you will not get the real-time data. It will remain open from 7:00 PM IST to 1:30 AM IST (IST is 9.5 hours ahead of EDT).

Read the data file generated inside the folder every minute and convert the data into DStreams in Spark
Using Spark Streaming, perform the following analyses:

1. Calculate the simple moving average closing price of the four stocks in a 5-minute sliding window for the last 10 minutes.  Closing prices are used mostly by the traders and investors as it reflects the price at which the market finally settles down. The SMA (Simple Moving Average) is a parameter used to find the average stock price over a certain period based on a set of parameters. The simple moving average is calculated by adding a stock's prices over a certain period and dividing the sum by the total number of periods. The simple moving average can be used to identify buying and selling opportunities

2. Find the stock out of the four stocks giving maximum profit (average closing price - average opening price) in a 5-minute sliding window for the last 10 minutes

3. Find out the Relative Strength Index or RSI of the four stocks in a 1-minute sliding window for the last 10 minutes. RSI is considered overbought when above 70 and oversold when below 30. 

4. Calculate the trading volume of the four stocks every 10 minutes and decide which stock to purchase out of the four stocks. Volume plays a very important role in technical analysis as it helps us to confirm trends and patterns. You can think of volumes as a means to gain insights into how other participants perceive the market. Volumes are an indicator of how many stocks are bought and sold over a given period of time. Higher the volume, more likely the stock will be bought

Build the Spark application using Maven. Generate a fat jar as required corresponding to the Spark application code to generate DStreams and perform the analyses. Run the python script and the Spark application fat jar and store the results of the analyses in the output file/s and take the screenshots of the console output for at least 30 minutes.

Write the logic in simple words for the Spark Application code developed and how to run it and the fat jar in a document.
