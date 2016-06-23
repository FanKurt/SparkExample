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

import com.imac.Retention.Retention;
import com.imac.tag.CatchyTag;

import scala.Tuple2;
import scala.Tuple3;

public class Active_Hourly {
	private static String date_1 = "2016-03-18 13:36:33" ;
	private static JavaPairRDD<String, Map<String, Object>>  esData ;
	private static JavaSparkContext sc ; 
	private static DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	private static Calendar cal = Calendar.getInstance();
	public Active_Hourly( JavaSparkContext sc, JavaPairRDD<String, Map<String, Object>>  esData){
		cal.add(Calendar.DATE, -1); //設定時間
		this.sc = sc;
		this.esData = esData;
		this.date_1 = dateFormat.format(new Date()); // 今日
//		this.date_2 = dateFormat.format(cal.getTime()); //昨日
	}
	public static void runHourly() throws SQLException {
		
		//找出每小時使用者 DAU活耀的情況
		JavaRDD<Object> hourOfDau =esData.map(new Function<Tuple2<String,Map<String,Object>>, Tuple3>() {
			public Tuple3 call(Tuple2<String, Map<String, Object>> arg0)throws Exception {
				try{
					String time = arg0._2.get("time").toString();
					String url = arg0._2.get("url").toString();
					String ip = arg0._2.get("ip").toString();
					
					return new Tuple3<String, String, String>(time , url , ip);
				}catch(Exception e){
					return new Tuple3<String, String,String>(null,null , null);
				}
			}
		}).filter(new Function<Tuple3, Boolean>() {
			public Boolean call(Tuple3 arg0) throws Exception {
				return  arg0._1()!=null && arg0._1().equals(date_1) && arg0._2().toString().contains("/user/checktoken");
			}
		}).mapToPair(new PairFunction<Tuple3, String, Integer>() {
			public Tuple2<String, Integer> call(Tuple3 arg0) throws Exception {
				return new Tuple2<String, Integer>(arg0._3().toString() , 1);
			}
		}).reduceByKey(new Function2<Integer, Integer, Integer>() {
			public Integer call(Integer arg0, Integer arg1) throws Exception {
				return arg0 + arg1;
			}
		}).mapToPair(new PairFunction<Tuple2<String,Integer>, String, Object>() {
			public Tuple2<String, Object> call(Tuple2<String, Integer> arg0)
					throws Exception {
				if(arg0._2()>=2){
					return new Tuple2<String, Object>(arg0._1(),2.0);
				}else{
					return new Tuple2<String, Object>(arg0._1(),1.0);
				}
			}
		}).values();
		
		
		JavaDoubleRDD doubleRDD = new JavaDoubleRDD(hourOfDau.rdd());
		
		System.out.println("----------------------------------------------");
		System.out.println("Answer Hourly DAU Count : "+doubleRDD.sum());
		System.out.println("----------------------------------------------");
		Active.saveToMySQL(Math.round(doubleRDD.sum()), CatchyTag.TABLE3_3);
		
	}
	
	private static Boolean isInTimeZone(String time , String time_zone){
		String [] dates = time.split(" ")[0].split("-");
		String [] date_zones = time_zone.split(" ")[0].split("-");
		
		String [] times = time.split(" ")[1].split(":");
		String [] time_zones = time_zone.split(" ")[1].split(":");
		
		//比較年份 時間點1
		if(Integer.parseInt(dates[0]) == Integer.parseInt(date_zones[0])){
			//比較月份 時間點
			if(Integer.parseInt(dates[1]) == Integer.parseInt(date_zones[1])){
				int date_day = Integer.parseInt(dates[2]);
				int date_zones_day = Integer.parseInt(date_zones[2]);
				//比較日子 時間點
				if(date_day == date_zones_day){
					int hour = Integer.parseInt(times[0]);
					int hour_zones = Integer.parseInt(time_zones[0]);
					if(hour <=hour_zones && hour >= (hour_zones-1)){
						return true;
					}
				}
				
			}
		}
		return false;
	}

}
