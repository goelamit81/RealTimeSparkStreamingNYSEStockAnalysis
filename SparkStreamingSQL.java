package com.upgrad.sparkstreamingsql;

import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;

/*
 * This is the main class which converts incoming stocks data into DStreams and perform 4 analysis as per 
 * problem statement using Spark Streaming:
 * 1. Simple Moving Average Closing Price
 * 2. Maximum Profitable Stock
 * 3. Relative Strength Index
 * 4. Highest Volume Traded
 */

public class SparkStreamingSQL {
	
	/*
	 * Main entry point in SparkStreaming: 
	 * 1.  Setup Spark configuration 
	 * 2.  Setup Streaming context with batch interval of 1 minute
	 * 3.  Setup Spark session
	 * 4.  Setup SQLContext 
	 * 5.  Setup check pointing
	 * 6.  Convert input data files into DStream  
	 * 7.  Create different window on DStream for different analysis based on Problem Statement 
	 * 8.  Perform all 4 analysis on respective window stream 
	 * 9.  Display and save output 
	 * 10. Start, wait for 30 minutes (and 10 seconds) before Termination and Close stream.
	 */	

	public static void main(String[] args) throws InterruptedException {
		
		// Check if 2 arguments are passed to the program
		if (args.length != 2) {
			System.out.println("Please enter 1st argument as path of input directory");
			System.out.println("Please enter 2nd argument as path of output directory");
			return;
		}		

		// Setup Spark configuration		
		SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("jsonstreaming");

		// Setup Streaming context with batch interval of 1 minute
		JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(60));

		// Setup Spark session
		SparkSession ss = SparkSession.builder().config(conf).getOrCreate();
		
		// Setup SQL context		
		SQLContext sc = ss.sqlContext();		

		// Setup logging to error only
		Logger.getRootLogger().setLevel(Level.ERROR);
		
		// Replace any forward slash ('/') at the end, passed in 1st argument and store as input path		
		String ipath = args[0].replaceAll("/$", "");
		
		// Replace any forward slash ('/') at the end, passed in 2nd argument and store as output path
		String opath = args[1].replaceAll("/$", "");		
		
		// Set up check pointing
		jsc.checkpoint(opath + "/checkpoint_dir");		
		
		// Convert input files into DStream
		JavaDStream<String> dstream = jsc.textFileStream(ipath).cache();

		/*
		 * Analysis - 1: Using Spark Streaming SQL
		 * 
		 * Simple Moving Average Closing Price for all stocks in the stream window:
		 * 1. On original dstream received from textFileStream, use window of 10 minutes with
		 *    sliding interval as 5 minute and use foreachRDD method.
		 * 2. Inside foreachRDD, check if rdd is not empty and then read and parse input rdd in the streaming window 
		 *    using json() method and convert into Dataset data. 
		 * 3. Select symbol and closing price from the data and create another Dataset data1.  
		 * 4. Create temporary view on data1 and name it as "simplemovingavg" so SQL like operations can be performed.
		 * 5. Using sql() method, in main select clause, select symbol and avg of closing price as avg_closing_price
		 *    from simplemovingavg group by symbol order by avg_closing_price in descending order and then symbol in 
		 *    ascending order and create another Dataset avgData.
		 * 6. Print "Simple Moving Average Closing Price" along with current timestamp
		 * 7. Show all records from avgData and this the average closing price for all stocks.
		 * 8. Coalesce all rdds into 1 partitions and order by avg_closing_price in descending order and then symbol in 
		 *    ascending order and save as text files in a sub directory under output directory.
		 */	
		dstream.window(Durations.seconds(600), Durations.seconds(300)).foreachRDD(new VoidFunction<JavaRDD<String>>() {
			private static final long serialVersionUID = 1L;
			@Override
			public void call(JavaRDD<String> rdd) {
				if (!rdd.isEmpty()) {
					
					Dataset<Row> data = sc.read().json(rdd);
					
					Dataset<Row> data1 = data.select(data.col("symbol"), data.col("priceData.close").cast("double"));				
					data1.createOrReplaceTempView("simplemovingavg");
					Dataset<Row> avgData = sc.sql("select symbol, avg(close) as avg_closing_price from simplemovingavg "
							                      + "group by symbol order by avg_closing_price desc, symbol asc");
					System.out.println("Simple Moving Average Closing Price : " + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()));						
					avgData.show();
					avgData.coalesce(1).orderBy(avgData.col("avg_closing_price").desc(), avgData.col("symbol").asc()).rdd()
					.saveAsTextFile(opath + "/sma_" + new SimpleDateFormat("yyyy-MM-dd_HH-mm-ss").format(new Date()));
				}
			}
		});		

		/*
		 * Analysis - 2: Using Spark Streaming SQL
		 * 
		 * Maximum Profitable Stock having highest (average closing price-average opening price) as profit 
		 * in the stream window: 
		 * 1. On original dstream received from textFileStream, use window of 10 minutes with
		 *    sliding interval as 5 minute and use foreachRDD method.
		 * 2. Inside foreachRDD, check if rdd is not empty and then read and parse input rdd in the streaming window 
		 *    using json() method and convert into Dataset data. 
		 * 3. Select symbol, closing price and opening price from the data and create another Dataset data2.  
		 * 4. Create temporary view on data2 and name it as "maxprofit" so SQL like operations can be performed.
		 * 5. Using sql() method, in from clause (select symbol, avg of closing price as avg_close and avg of opening 
		 *    price as avg_open from maxprofit group by symbol) and then in main select clause, select symbol, avg_close,
		 *    avg_open and avg_close-avg_open as profit order by profit in descending order and then symbol in 
		 *    ascending order and create another Dataset profitData.
		 * 6. Print "Maximum Profitable Stock"  along with current timestamp
		 * 7. Show top first record from profitData and this the stock having maximum profit
		 * 8. Coalesce all rdds into 1 partitions and order by profit in descending order and then symbol in 
		 *    ascending order and save as text files in a sub directory under output directory.
		 */	
		dstream.window(Durations.seconds(600), Durations.seconds(300)).foreachRDD(new VoidFunction<JavaRDD<String>>() {
			private static final long serialVersionUID = 1L;
			@Override
			public void call(JavaRDD<String> rdd) {
				if (!rdd.isEmpty()) {
					
					Dataset<Row> data = sc.read().json(rdd);
					
					Dataset<Row> data2 = data.select(data.col("symbol"), data.col("priceData.close").cast("double"), 
							                         data.col("priceData.open").cast("double"));
					data2.createOrReplaceTempView("maxprofit");
					Dataset<Row> profitData = sc.sql("select symbol, avg_close, avg_open, avg_close-avg_open as profit from "
							                         + "(select symbol, avg(close) as avg_close, avg(open) as avg_open from "
							                         + "maxprofit group by symbol) order by profit desc, symbol asc");
					System.out.println("Maximum Profitable Stock : " + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()));					
					profitData.show(1);
					profitData.coalesce(1).orderBy(profitData.col("profit").desc(), profitData.col("symbol").asc()).rdd()
					.saveAsTextFile(opath + "/mps_" + new SimpleDateFormat("yyyy-MM-dd_HH-mm-ss").format(new Date()));
				}
			}
		});	
		
		/*
		 * Analysis - 3: Using Spark Streaming SQL
		 * 
		 * Relative Strength Index (RSI) for each stock in the stream window:
		 * 1.  On original dstream received from textFileStream, use window of 10 minutes with
		 *     sliding interval as 1 minute and use foreachRDD method.
		 * 2.  Inside foreachRDD, check if count >= 10 and then read and parse input rdd in the streaming window using 
		 *     json() method and convert into Dataset data. 
		 * 3.  Select symbol, timestamp and closing price from the data and create another Dataset data3.  
		 * 4.  Create temporary view on data3 and name it as "strengthindex" so SQL like operations can be performed.
		 * 5.  Using sql() method, in with clause, (first select * and row_number() partition by symbol, order by timestamp 
		 *     so record with latest timestamp appears in last) and then in main select clause, 
		 *     (compute gain and loss based on closing price of previous record and current record by joining on data of 
		 *     previous record and current record and create another Dataset gainLossData.
		 * 6.  Create temporary view on gainLossData and name it as "gainlossindex" so SQL like operations can be performed.
		 * 7.  Using sql() method, select symbol, average gain and average loss from gainlossindex group by symbol and create 
		 *     another Dataset avgGainLossData.
		 * 8.  Create temporary view on avgGainLossData and name it as "strengthindex2" so SQL like operations can be performed.
		 * 9.  Using sql() method, select symbol and compute rsi:
		 *     (a) If avg gain is 0 then rsi is 0
		 *     (b) If avg loss is 0 then rsi is 100
		 *     (c) Otherwise compute rsi using the formula mentioned in the problem statement for rsi
		 *     and order by rsi in descending order and create another Dataset rsiData. 
		 * 10. Print "Relative Strength Index"  along with current timestamp
		 * 11. Show data from rsiData.
		 * 12. Coalesce all rdds into 1 partitions and order by rsi in descending order and save as text 
		 *     files in a sub directory under output directory.
		 */						
		dstream.window(Durations.seconds(600), Durations.seconds(60)).foreachRDD(new VoidFunction<JavaRDD<String>>() {
			private static final long serialVersionUID = 1L;
			@Override
			public void call(JavaRDD<String> rdd) {
				if (rdd.count() >= 10) {
					
					Dataset<Row> data = sc.read().json(rdd);
					
					Dataset<Row> data3 = data.select(data.col("symbol"), data.col("timestamp"), data.col("priceData.close").cast("double"));
					data3.createOrReplaceTempView("strengthindex");
					Dataset<Row> gainLossData = sc
									.sql("with sym_rn as "
									     + "(select t.*, row_number() over (partition by symbol order by timestamp) as rn from strengthindex t) "
										 + "select case when old.close - new.close > 0 then old.close - new.close else 0 end as gain, "
										 + "case when old.close - new.close < 0 then new.close - old.close else 0 end as loss, "
										 + "old.symbol, old.timestamp "
										 + "from sym_rn as old left join sym_rn new on "
										 + "new.symbol = old.symbol and new.rn = old.rn - 1");
					gainLossData.createOrReplaceTempView("gainlossindex");									
					Dataset<Row> avgGainLossData = sc.sql("select symbol, avg(gain) as avg_gain, avg(loss) as avg_loss "
							                               + "from gainlossindex group by symbol");
					avgGainLossData.createOrReplaceTempView("strengthindex2");
					Dataset<Row> rsiData = sc.sql("select symbol, case when avg_gain == 0 then 0 when avg_loss == 0 then 100 "
							                       + "else (100 - (100/(1+(avg_gain/avg_loss)))) end as rsi "
							                       + "from strengthindex2 order by rsi desc, symbol asc");
					System.out.println("Relative Strength Index : " + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()));
					rsiData.show();
					rsiData.coalesce(1).orderBy(rsiData.col("rsi").desc(), rsiData.col("symbol").asc()).rdd()
					.saveAsTextFile(opath + "/rsi_" + new SimpleDateFormat("yyyy-MM-dd_HH-mm-ss").format(new Date()));
				}
			}
		});
		
		/*
		 * Analysis - 4: Using Spark Streaming SQL
		 * 
		 * Highest Trading Volume stock in the stream window to buy:
		 * 1. On original dstream received from textFileStream, use window of 10 minutes with
		 *    sliding interval as 10 minute and use foreachRDD method.
		 * 2. Inside foreachRDD, check if rdd is not empty and then read and parse input rdd in the streaming window 
		 *    using json() method and convert into Dataset data. 
		 * 3. Select symbol and volume from the data and create another Dataset data4.  
		 * 4. Create temporary view on data4 and name it as "maxvolume" so SQL like operations can be performed.
		 * 5. Using sql() method, in main select clause, select symbol and sum of volume as total_volume from maxvolume 
		 *    group by symbol order by total_volume in descending order and then symbol in ascending order and create 
		 *    another Dataset volumeData.
		 * 6. Print "Buy this Stock having Highest Volume Traded"  along with current timestamp
		 * 7. Show top first record from volumeData and this the stock to buy.
		 * 8. Coalesce all rdds into 1 partitions and order by total_volume in descending order and then symbol in 
		 *    ascending order and save as text files in a sub directory under output directory.
		 */		
		dstream.window(Durations.seconds(600), Durations.seconds(600)).foreachRDD(new VoidFunction<JavaRDD<String>>() {
			private static final long serialVersionUID = 1L;
			@Override
			public void call(JavaRDD<String> rdd) {
				if (!rdd.isEmpty()) {
					
					Dataset<Row> data = sc.read().json(rdd);
					
					Dataset<Row> data4 = data.select(data.col("symbol"), data.col("priceData.volume").cast("long"));
					data4.createOrReplaceTempView("maxvolume");
					Dataset<Row> volumeData = sc.sql("select symbol, sum(volume) as total_volume from maxvolume group by symbol "
							                          + "order by total_volume desc, symbol asc");
					System.out.println("Buy this Stock having Highest Volume Traded : " + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()));
					volumeData.show(1);
					volumeData.coalesce(1).orderBy(volumeData.col("total_volume").desc(), volumeData.col("symbol").asc()).rdd()
					.saveAsTextFile(opath + "/hvt_" + new SimpleDateFormat("yyyy-MM-dd_HH-mm-ss").format(new Date()));					
				}
			}
		});
		
		// Print current timestamp before starting
		System.out.println("\nStart Time : " + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()) + "\n");
		
		// Start streaming, wait for 30 minutes (and 10 seconds) before termination and then close the stream		
		jsc.start();
		jsc.awaitTerminationOrTimeout(1810000);
		
		// Print current timestamp before closing 
		System.out.println("\nEnd Time : " + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()) + "\n");		
		jsc.close();
	}
}


