package com.imac;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import javax.mail.Store;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.serializer.KryoSerializer;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;
import org.json.simple.JSONObject;
import scala.Tuple2;

public class Elasticsearch {
	// private static DateFormat dateFormat = new
	// SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
	private static Calendar cal = Calendar.getInstance();
	private static org.apache.log4j.Logger LOGGER = org.apache.log4j.Logger.getLogger(Elasticsearch.class); 
	private static DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
	public static void main(final String[] args) {

//		if (args.length < 1) {
//			System.out.println("Format Error : [Receive ES path]");
//		}
		SparkConf conf = new SparkConf().setMaster("local[2]").setAppName(
				"Elasticsearch");
		conf.set("spark.serializer", KryoSerializer.class.getName());
		conf.set("es.index.auto.create", "true");
		conf.set("es.nodes", "10.26.1.53:9200");
		conf.set("es.resource", args[0]);
		conf.set("es.input.json" , "true");
		
		JavaSparkContext sc = new JavaSparkContext(conf);
		long pre_start = System.currentTimeMillis();
		JavaPairRDD<String, Map<String, Object>> esRDD = JavaEsSpark.esRDD(sc);
		
		
//		if(Integer.parseInt(args[2])==0){
//			esRDD.map(new Function<Tuple2<String,Map<String,Object>>, String>() {
//				public String call(Tuple2<String, Map<String, Object>> arg0)
//						throws Exception {
//					return new JSONObject(arg0._2).toJSONString();
//				}
//			}).saveAsTextFile("/estest");
//			long pre_end = System.currentTimeMillis();
//			System.out.println((pre_end-pre_start)/1000+"	s");
//		}
//		
//		if(Integer.parseInt(args[2])==1){
//		JavaRDD<String> jsonRDD = esRDD.map(new Function<Tuple2<String,Map<String,Object>>, String>() {
//		public String call(Tuple2<String, Map<String, Object>> arg0)
//				throws Exception {
//			JSONObject json = new JSONObject();
//			json.putAll(arg0._2);
//			return json.toString();
//		}
//	});
//	
//	JavaEsSpark.saveToEs(jsonRDD, "imac/test");
//			long end = System.currentTimeMillis();
//			System.out.println((end-pre_start)/1000+"	s");
//		}
		
		for(Tuple2<String, Map<String, Object>> v : esRDD.collect()){
			System.out.println(v._2);
		}
		
		
	}

	private static JavaPairRDD<String, Integer> getFilterRDD(SparkConf conf ,JavaSparkContext sc , String key) {
		ArrayList<String> arList = new ArrayList<>();
		arList.add(key);
		JavaRDD<String> filterString = sc.parallelize(arList);
		JavaPairRDD<String, Integer> keyword = filterString.mapToPair(new PairFunction<String, String, Integer>() {
			public Tuple2<String, Integer> call(String arg0)throws Exception {
				return new Tuple2<String, Integer>(arg0, 1);
			}
		});
		return keyword;
	}
	
	private static String MaptoJson(Tuple2<String, Map<String, Object>> arg0){
		JSONObject json = new JSONObject();
		Map<String, Object> map = arg0._2;
		json.putAll(map);
		json.put("@timestamp", getJsonTime(map.get("@timestamp")));
		return json.toString();
	}
	
	private static String getJsonTime(Object object){
		String [] token = object.toString().split(" ");
		String [] time = token[3].split(":");
		String current_time = token[token.length-1]+"-"+tranferMonth(token[1])+"-"+token[2]+"T"+token[3]+"Z";
		return current_time;
	}
	private static String tranferMonth(String month){
		String [] arrStrings = {"Jan","Feb","Mar","Apr","Mar","Jun","Jul","Aug","Sep","Oct","Nov","Dec"};
		for(int i=0 ;i <arrStrings.length ;i++){
			if(month.equals(arrStrings[i])){
				return (i<9)?"0"+(i+1):""+(i+1);
			}
		}
		return "";
	}
}
