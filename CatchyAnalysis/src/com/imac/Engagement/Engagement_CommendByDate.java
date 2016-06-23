package com.imac.Engagement;

import java.sql.SQLException;
import java.text.DateFormat;
import java.text.ParseException;
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

public class Engagement_CommendByDate {
	private static JavaPairRDD<String, Map<String, Object>>  esData ;
	private static JavaSparkContext sc ; 
	private static DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
	private static Calendar todayCalendar = Calendar.getInstance();
	private static Calendar weekCalendar = Calendar.getInstance();
	private static Calendar monthCalendar = Calendar.getInstance();
	private static Calendar dataCalendar = Calendar.getInstance();
	private static final Pattern commend = Pattern.compile("/message/main/(\\d*)/(.*)");
	private static String todayDate = "2016-03-18" ;
	private static String dateType = "" ;
	
	/**
	 * @param sc JavaSparkContext
	 * @param esData Elasticsearch資料
	 * @param dateType 0/1/2 當日/週/月
	 * @throws ParseException 
	 */
	public Engagement_CommendByDate( JavaSparkContext sc, JavaPairRDD<String, Map<String, Object>>  esData,String dateType) throws ParseException{
		this.sc = sc;
		this.esData = esData;
		this.dateType = dateType;
		this.todayDate = dateFormat.format(new Date()); // 今日
		setCalenderTime(dateType);
	}
	
	public static void run() throws NumberFormatException, SQLException {
		
		JavaRDD<Tuple2<String,String>> url_rdd =esData.map(new Function<Tuple2<String,Map<String,Object>>, Tuple2<String,String>>() {
			public Tuple2<String,String> call(Tuple2<String, Map<String, Object>> arg0)
					throws Exception {
				try{
					String url = arg0._2.get("url").toString();
					String time = arg0._2.get("time").toString();
					String [] times = time.split(" ");
					return new Tuple2<String, String>(url,times[0]);
				}catch(Exception e){
					return null;
				}
			}
		}).filter(new Function<Tuple2<String,String>, Boolean>() {
			public Boolean call(Tuple2<String, String> arg0) throws Exception {
				return arg0!=null && isInTimeZone(dateType, arg0._2);
			}
		});
		
		
		JavaPairRDD<Object, String> result_rdd = url_rdd.mapToPair(new PairFunction<Tuple2<String,String>, String, Object>() {
			public Tuple2<String, Object> call(Tuple2<String,String> arg0) throws Exception {
				Matcher m = commend.matcher(arg0._1);
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
		
		
		String finalResult="";
		//若分析結果為0，則輸出0
		for(Tuple2<Object, String> result : result_rdd.take(10)){
			try{
				System.out.println("----------------------------------------------");
				System.out.println("Answer  most Commend Count in Different of Date: "+result._2);
				System.out.println("----------------------------------------------");
				finalResult = result._2;
				Engagement.saveToMySQL(finalResult, CatchyTag.TABLE5_7 , Integer.parseInt(dateType));
			}catch(Exception e){
				System.out.println("----------------------------------------------");
				System.out.println("Answer most Like Commend in Different of Date: 0");
				System.out.println("----------------------------------------------");
				finalResult = "0";
			}
			
		}
	}
	/**
	 * 
	 * @param dateType 0/1/2 當日/週/月
	 * @param dataDate 今日日期
	 * @return 是否包含在時間區間內
	 * @throws ParseException 
	 */
	private static Boolean isInTimeZone(String dateType , String dataDate) throws ParseException{
		try{
			dataCalendar.setTime(dateFormat.parse(dataDate));
			if(dateType.equals("0")){
				return todayCalendar.equals(dataCalendar);
			}else if(dateType.equals("1")){
				return weekCalendar.before(dataCalendar) && todayCalendar.after(dataCalendar);
			}else{
				return monthCalendar.before(dataCalendar) && todayCalendar.after(dataCalendar);
			}
		}catch(Exception e){
			return false;
		}
	}
	
	/**
	 * @param dateType 0/1/2 當日/週/月
	 * @throws ParseException
	 */
	private void setCalenderTime(String dateType) throws ParseException {
		if(dateType.equals("0")){
			todayCalendar.setTime(dateFormat.parse(todayDate));
			System.out.println("today : "+todayCalendar.getTime());
		}else if(dateType.equals("1")){
			todayCalendar.setTime(dateFormat.parse(todayDate));
			todayCalendar.add(Calendar.DATE, +1);
			System.out.println("today : "+todayCalendar.getTime());
			weekCalendar.setTime(dateFormat.parse(todayDate));
			weekCalendar.add(Calendar.DATE, -7);
			System.out.println("week : "+weekCalendar.getTime());
		}else{
			todayCalendar.setTime(dateFormat.parse(todayDate));
			todayCalendar.add(Calendar.DATE, +1);
			System.out.println("today : "+todayCalendar.getTime());
			monthCalendar.setTime(dateFormat.parse(todayDate));
			monthCalendar.add(Calendar.MONTH, -1);
			System.out.println("month"+monthCalendar.getTime());
		}
	}
}
