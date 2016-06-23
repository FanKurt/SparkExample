package com.imac.Retention;

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

public class Retention_Weekly {
	private static String todayDate = "2016-03-18";
	private static JavaPairRDD<String, Map<String, Object>>  esData ;
	private static JavaSparkContext sc ; 
	private static DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
	private static Calendar todayCalendar = Calendar.getInstance();
	private static Calendar weekCalendar = Calendar.getInstance();
	private static Calendar dataCalendar = Calendar.getInstance();
	public Retention_Weekly( JavaSparkContext sc, JavaPairRDD<String, Map<String, Object>>  esData) throws ParseException{
		this.sc = sc;
		this.esData = esData;
		this.todayDate = dateFormat.format(new Date()); // 今日
		setCalenderTime();
	}
	public static void runWeekly() throws SQLException {
		
		//篩選昨日使用者 active的情況
		JavaRDD<String> week_uer_rdd =esData.map(new Function<Tuple2<String,Map<String,Object>>, Tuple3>() {
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
				return arg0._1()!=null && isInThisWeek(arg0._1().toString()) 
						&& arg0._2().toString().contains("/user/checktoken");
			}
		}).map(new Function<Tuple3, String>() {
			public String call(Tuple3 arg0) throws Exception {
				return arg0._3().toString();
			}
		});
		
		week_uer_rdd.cache();
		
		long result = week_uer_rdd.count();
		
		System.out.println("----------------------------------------------");
		System.out.println("Answer Weekly Retain Count : "+week_uer_rdd.count());
		System.out.println("----------------------------------------------");
		
		Retention.saveToMySQL(result, CatchyTag.TABLE2_3);
	}
	
	private static Boolean isInThisWeek(String dataDate) throws ParseException{
		try{
			dataCalendar.setTime(dateFormat.parse(dataDate));
			return weekCalendar.before(dataCalendar) && todayCalendar.after(dataCalendar);
		}catch(Exception e){
			return false;
		}
	}
	
	private void setCalenderTime() throws ParseException {
		todayCalendar.setTime(dateFormat.parse(todayDate));
		todayCalendar.add(Calendar.DATE, +1);
		System.out.println("today : "+todayCalendar.getTime());
		weekCalendar.setTime(dateFormat.parse(todayDate));
		weekCalendar.add(Calendar.DATE, -7);
		System.out.println("lastweek : "+weekCalendar.getTime());
	}
}
