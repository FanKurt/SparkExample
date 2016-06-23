package com.imac.Engagement;

import java.sql.SQLException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;

import com.imac.tag.CatchyTag;

import akka.dispatch.Filter;

import scala.Tuple2;
import scala.Tuple3;

public class Engagement_Follow {
	private static JavaPairRDD<String, Map<String, Object>>  esData ;
	private static JavaSparkContext sc ; 
	private static DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
	private static Calendar cal = Calendar.getInstance();
	private static final Pattern follow = Pattern.compile("/follow/(.*)()");
	public Engagement_Follow( JavaSparkContext sc, JavaPairRDD<String, Map<String, Object>>  esData){
		this.sc = sc;
		this.esData = esData;
	}
	public static void runWeekly() throws SQLException {
		
		JavaRDD<String> url_rdd =esData.map(new Function<Tuple2<String,Map<String,Object>>, String>() {
			public String call(Tuple2<String, Map<String, Object>> arg0)
					throws Exception {
				try{
					String url = arg0._2.get("url").toString();
					return url;
				}catch(Exception e){
					return null;
				}
			}
		}).filter(new Function<String, Boolean>() {
			public Boolean call(String arg0) throws Exception {
				return arg0!=null;
			}
		});
		
		
		JavaPairRDD<Object, String> result_rdd = url_rdd.mapToPair(new PairFunction<String, String, Object>() {
			public Tuple2<String, Object> call(String arg0) throws Exception {
				Matcher m = follow.matcher(arg0);
				if(m.find()){
					return new Tuple2<String, Object>(m.group(1).toString(), 1);
				}
				return null;
			}
		}).filter(new Function<Tuple2<String,Object>, Boolean>() {
			public Boolean call(Tuple2<String, Object> arg0) throws Exception {
				return arg0!=null;
			}
		}).reduceByKey(new Function2<Object, Object, Object>() {
			public Object call(Object arg0, Object arg1) throws Exception {
				return (int)arg0+(int)arg1;
			}
		}).mapToPair(new PairFunction<Tuple2<String,Object>, Object, String>() {
			public Tuple2<Object, String> call(Tuple2<String, Object> arg0)
					throws Exception {
				return arg0.swap();
			}
		}).sortByKey(false);
		
		
	
		String result="";
		for(Tuple2<Object, String> value : result_rdd.take(10)){
			try{
				System.out.println(value._2+"");
				result = value._2();
				Engagement.saveToMySQL(result, CatchyTag.TABLE5_4);
			}catch(Exception e){
				result ="none";
			}
		}
		
		
	}

}
