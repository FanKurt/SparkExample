package com.imac;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.serializer.KryoSerializer;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;
import org.json.JSONObject;

import com.cloudera.sparkts.DayFrequency;
import com.cloudera.sparkts.HourFrequency;
import com.cloudera.sparkts.UniformDateTimeIndex;
import com.cloudera.sparkts.api.java.DateTimeIndexFactory;
import com.cloudera.sparkts.api.java.JavaTimeSeriesRDD;
import com.cloudera.sparkts.api.java.JavaTimeSeriesRDDFactory;

import scala.Tuple2;
import scala.collection.Iterator;


public class Main {

	public static void main(String[] args) {
		if (args.length < 1) {
			System.out.println("Format Error : [Receive ES path]");
		}
		SparkConf conf = new SparkConf().setMaster("local[2]").setAppName(
				"Elasticsearch");
		conf.set("spark.serializer", KryoSerializer.class.getName());
		conf.set("es.nodes", "10.26.1.9:9200");
		conf.set("es.input.json" , "true");
		
		JavaSparkContext sc = new JavaSparkContext(conf);
		SQLContext sqlContext =new SQLContext(sc);
		
		JavaPairRDD<String, Map<String, Object>> esRDD = JavaEsSpark.esRDD(sc,args[0]);
		JavaRDD<String> message = esRDD.map(new Function<Tuple2<String,Map<String,Object>>, String>() {
			public String call(Tuple2<String, Map<String, Object>> arg0)
					throws Exception {
				return arg0._2.get("message").toString();
			}
		}).filter(new Function<String, Boolean>() {
			public Boolean call(String arg0) throws Exception {
				return arg0.contains("op/s");
			}
		});
		
		JavaRDD<CephDate> cephRDD = message.map(new Function<String, CephDate>() {
			public CephDate call(String arg0) throws Exception {
				String[] token = arg0.split(" ");

				String symbol = token[2];
				double price = Double.parseDouble(token[token.length-5]);
				String [] date = token[0].split("-");
				String [] time = token[1].split(":");
				ZonedDateTime zonedDateTime = ZonedDateTime.of(Integer.parseInt(date[0]), Integer.parseInt(date[1]), Integer.parseInt(date[2])
						, 0, 0, 0, 0,
						ZoneId.systemDefault());
				CephDate mDate = new CephDate();
				mDate.setTimestamp(zonedDateTime.toString());
//				mDate.setTimestamp(token[0]+" "+token[1]);
				mDate.setIP(symbol);
				mDate.setOPS(price);

				return mDate;
			}
		});
		
//	    List<CephDate> list2 = cephRDD.collect();
//		
//		for(int i=0 ;i <list2.size();i++){
//			System.out.println(list2.get(i).getTimestamp());
//		}
		
		DataFrame cephDataFrame = sqlContext.createDataFrame(cephRDD, CephDate.class);

		System.out.println("csvDataFrame  " + cephDataFrame);

		ZonedDateTime start = ZonedDateTime.of(2016, 1, 25, 0,0,0,0, ZoneId.systemDefault());
		ZonedDateTime end = ZonedDateTime.of(2016, 1, 27,0,0,0,0, ZoneId.systemDefault());

		UniformDateTimeIndex dateTimeIndex = DateTimeIndexFactory.uniformFromInterval(start, end, new DayFrequency(1));
		
		
		Iterator<ZonedDateTime> aa = dateTimeIndex.zonedDateTimeIterator();
		
		for(int i=0 ; aa.hasNext() ;i++){
			System.out.println("csvDataFrame  " + aa.next());
		}
	
		JavaTimeSeriesRDD<String> tickerRDD = JavaTimeSeriesRDDFactory
				.javaTimeSeriesRDDFromObservations(dateTimeIndex, cephDataFrame, "timestamp", "IP", "OPS");
		
//		
//		tickerRDD.distinct().foreach(new VoidFunction<Tuple2<String,Vector>>() {
//			public void call(Tuple2<String, Vector> arg0) throws Exception {
//				System.out.println("tickerRDD  " + arg0);
//			}
//		});
		
		
		System.out.println(""+tickerRDD.name());
		
		JavaPairRDD<String, Vector> cacheRDD = tickerRDD.cache();
		
		System.out.println("tickerRDD  " + cacheRDD.count());
		
		List<Tuple2<String, Vector>> list = cacheRDD.collect();
		
		for(int i=0 ;i <list.size();i++){
			System.out.println(list.get(i));
		}
		
	
	
		
	}

}
