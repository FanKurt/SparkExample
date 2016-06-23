package com.imac.Active;

import java.sql.SQLException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;

import com.imac.tag.CatchyTag;

import scala.Tuple2;
import scala.Tuple3;
import scala.collection.generic.BitOperations.Int;

public class Active_Monthly {
	private static String date_1 = "2016-03-18" ;
	private static JavaPairRDD<String, Map<String, Object>>  esData ;
	private static JavaSparkContext sc ; 
	private static DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
	private static Calendar cal = Calendar.getInstance();
	public Active_Monthly( JavaSparkContext sc, JavaPairRDD<String, Map<String, Object>>  esData){
		cal.add(Calendar.DATE, -1); //設定時間
		this.sc = sc;
		this.esData = esData;
		this.date_1 = dateFormat.format(new Date()); // 今日
//		this.date_2 = dateFormat.format(cal.getTime()); //昨日
	}
	public static void runMonthly() throws SQLException {
		
		//找出每小時使用者 DAU活耀的情況 
		JavaPairRDD<String, Integer> monthOfDau =esData.map(new Function<Tuple2<String,Map<String,Object>>, Tuple3>() {
			public Tuple3 call(Tuple2<String, Map<String, Object>> arg0)throws Exception {
				try{
					String time = arg0._2.get("time").toString();
					String url = arg0._2.get("url").toString();
					String ip = arg0._2.get("ip").toString();
					String [] tokens = time.split("-");
					return new Tuple3<Integer, String, String>(Integer.parseInt(tokens[1]) , url , ip);
				}catch(Exception e){
					return new Tuple3<String, String,String>(null,null , null);
				}
			}
		}).filter(new Function<Tuple3, Boolean>() {
			public Boolean call(Tuple3 arg0) throws Exception {
				String [] tokens = date_1.split("-");
				return  arg0._1()!=null && ((int)arg0._1() == Integer.parseInt(tokens[1])) 
						&& !arg0._2().toString().equals("");
			}
		}).mapToPair(new PairFunction<Tuple3, String, Integer>() {
			public Tuple2<String, Integer> call(Tuple3 arg0) throws Exception {
				return new Tuple2<String, Integer>(arg0._3().toString() , 1);
			}
		}).reduceByKey(new Function2<Integer, Integer, Integer>() {
			public Integer call(Integer arg0, Integer arg1) throws Exception {
				return arg0 + arg1;
			}
		});
		
		//同一個用戶產生一次以上的session視為「2個active用戶」
		JavaRDD<Object> count = monthOfDau.mapToPair(new PairFunction<Tuple2<String,Integer>, String, Object>() {
			public Tuple2<String, Object> call(Tuple2<String, Integer> arg0)
					throws Exception {
				if(arg0._2()>=2){
					return new Tuple2<String, Object>(arg0._1(),2.0);
				}else{
					return new Tuple2<String, Object>(arg0._1(),1.0);
				}
			}
		}).values();
		
		JavaDoubleRDD doubleRDD = new JavaDoubleRDD(count.rdd());
		
		
//		System.out.println("----------------------------------------------");
//		System.out.println("Answer Monthly DAU Count : "+doubleRDD.sum());
//		System.out.println("----------------------------------------------");
//		Active.saveToMySQL(Math.round(doubleRDD.sum()), CatchyTag.TABLE3_4);
		System.out.println("----------------------------------------------");
		System.out.println("Answer Monthly DAU Distinct Count : "+monthOfDau.count());
		System.out.println("----------------------------------------------");
		Active.saveToMySQL(monthOfDau.count(), CatchyTag.TABLE3_4);
	}
	
}
