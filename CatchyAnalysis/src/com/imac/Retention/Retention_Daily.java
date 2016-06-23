package com.imac.Retention;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;

import com.imac.tag.CatchyTag;

import scala.Tuple2;
import scala.Tuple3;

public class Retention_Daily {
	private static String todayDate = "2016-03-18" ;
	private static JavaPairRDD<String, Map<String, Object>>  esData ;
	private static JavaSparkContext sc ; 
	private static DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
	private static Calendar todayCalendar = Calendar.getInstance();
	private static Calendar yesterCalendar = Calendar.getInstance();
	private static Calendar dataCalendar = Calendar.getInstance();
	public Retention_Daily( JavaSparkContext sc, JavaPairRDD<String, Map<String, Object>>  esData) throws ParseException{
		this.sc = sc;
		this.esData = esData;
		this.todayDate = dateFormat.format(new Date()); // 今日
		setCalenderTime();
	}
	
	public static void runDaily() throws SQLException {
		
		//篩選昨日使用者 active的情況
		JavaRDD<String> yesterday_user_rdd =esData.map(new Function<Tuple2<String,Map<String,Object>>, Tuple3>() {
			public Tuple3 call(Tuple2<String, Map<String, Object>> arg0)throws Exception {
				try{
					String time = arg0._2.get("time").toString();
					String url = arg0._2.get("url").toString();
					String ip = arg0._2.get("ip").toString();
					String [] tokens = time.split(" ");
					
					return new Tuple3<String, String, String>(tokens[0] , url , ip);
				}catch(Exception e){
					return new Tuple3<String, String,String>(null,null , null);
				}
			}
		}).filter(new Function<Tuple3, Boolean>() {
			public Boolean call(Tuple3 arg0) throws Exception {
				return  arg0._1()!=null && isInYesterday(arg0._1().toString()) && arg0._2().toString().contains("/user/checktoken");
			}
		}).map(new Function<Tuple3, String>() {
			public String call(Tuple3 arg0) throws Exception {
				return arg0._3().toString();
			}
		});
		
		yesterday_user_rdd.cache();
		
		//將其存成陣列
		final Broadcast<List<String>> user_list = sc.broadcast(yesterday_user_rdd.collect());
		
		//篩選今日使用者 active的情況並再篩選昨日與今日也有active的情況
		JavaRDD<Tuple3> today_user_rdd =esData.map(new Function<Tuple2<String,Map<String,Object>>, Tuple3>() {
			public Tuple3 call(Tuple2<String, Map<String, Object>> arg0)
					throws Exception {
				try{
					String time = arg0._2.get("time").toString();
					String url = arg0._2.get("url").toString();
					String ip = arg0._2.get("ip").toString();
					String [] tokens = time.split(" ");
					
					return new Tuple3<String, String, String>(tokens[0] , url , ip);
				}catch(Exception e){
					return new Tuple3<String, String,String>(null,null , null);
				}
			}
		}).filter(new Function<Tuple3, Boolean>() {
			public Boolean call(Tuple3 arg0) throws Exception {
				return  arg0._1()!=null && isInToday(arg0._1().toString()) && arg0._2().toString().contains("/user/checktoken");
			}
		}).filter(new Function<Tuple3, Boolean>() {
			public Boolean call(Tuple3 arg0) throws Exception {
				for(String v : user_list.getValue()){
					if(v.equals(arg0._3().toString())){
						return true;
					}
				}
				return false;
			}
		});
		
		today_user_rdd.cache();
		
		System.out.println("----------------------------------------------");
		System.out.println("Answer Daily Retain Count : "+today_user_rdd.count());
		System.out.println("----------------------------------------------");
		
		Retention.saveToMySQL(today_user_rdd.count(), CatchyTag.TABLE2_1);
		
	}
	
	private static Boolean isInYesterday(String dataDate) throws ParseException{
		try{
			dataCalendar.setTime(dateFormat.parse(dataDate));
			return yesterCalendar.equals(dataCalendar);
		}catch(Exception e){
			return false;
		}
	}
	
	private static Boolean isInToday(String dataDate) throws ParseException{
		try{
			dataCalendar.setTime(dateFormat.parse(dataDate));
			return todayCalendar.equals(dataCalendar);
		}catch(Exception e){
			return false;
		}
	}
	
	private void setCalenderTime() throws ParseException {
		todayCalendar.setTime(dateFormat.parse(todayDate));
		System.out.println("today : "+todayCalendar.getTime());
		yesterCalendar.setTime(dateFormat.parse(todayDate));
		yesterCalendar.add(Calendar.DATE, -1);
		System.out.println("yesterday : "+yesterCalendar.getTime());
	}

}
