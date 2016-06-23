package com.imac.Engagement;

import java.sql.SQLException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import com.imac.tag.CatchyTag;

import scala.Tuple2;
import scala.Tuple3;

public class Engagement_DocumentTypeByDate {
	private static String todayDate = "2016-03-18" ;
	private static JavaPairRDD<String, Map<String, Object>> esData ;
	private static JavaSparkContext sc ; 
	private static DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
	private static final Pattern product = Pattern.compile("/product/(\\d*)()");
	private static Calendar todayCalendar = Calendar.getInstance();
	private static Calendar dataCalendar = Calendar.getInstance();
	public Engagement_DocumentTypeByDate( JavaSparkContext sc, JavaPairRDD<String, Map<String, Object>>  esData) throws ParseException{
		this.sc = sc;
		this.esData = esData;
		this.todayDate = dateFormat.format(new Date()); // 今日
		setCalenderTime();
	}
	public static void run() throws SQLException {
		JavaPairRDD<String, Integer> message_rdd = esData.map(new Function<Tuple2<String,Map<String,Object>>, Tuple3<String,String,String>>() {
			public Tuple3<String,String,String> call(
					Tuple2<String, Map<String, Object>> arg0) throws Exception {
				try{
					String message = arg0._2().get("message").toString();
					String formatMessage = message.substring(message.indexOf("{")+1, message.lastIndexOf("}"));
					String url = arg0._2().get("url").toString();
					String time = arg0._2().get("time").toString();
					return new Tuple3<String,String,String>(getDataFormat(formatMessage),url, time);
				}catch(Exception e){
					return null;
				}
		
			}
		}).filter(new Function<Tuple3<String,String,String>, Boolean>() {
			public Boolean call(Tuple3<String,String,String> arg0) throws Exception {
				return arg0!=null && product.matcher(arg0._2()).find()
						&& arg0._1().contains("Type") && isInToday(arg0._3());
			}
		}).mapToPair(new PairFunction<Tuple3<String,String,String>, String, Integer>() {
			public Tuple2<String, Integer> call(Tuple3<String,String,String> arg0)
					throws Exception {
				String[] message = arg0._1().split(",");
				for(String value : message){
					if(value.contains("Type")){
						return new Tuple2<String, Integer>(value, 1);
					}
				}
				return null;
			}
		});
		
		
		JavaPairRDD<String, Integer> sum_rdd =message_rdd.reduceByKey(new Function2<Integer, Integer, Integer>() {
			public Integer call(Integer arg0, Integer arg1) throws Exception {
				return arg0+arg1;
			}
		});
		
		List<Tuple2<String, Integer>> list = sum_rdd.collect();
		Engagement.saveToMySQL(getResultArray(list, 0),
							   getResultArray(list, 1),
							   getResultArray(list, 2),
							   getResultArray(list, 3),
							   getResultArray(list, 4),
							   CatchyTag.TABLE5_12);
	}
	
	private static String getDataFormat(String message){
		return message.replace(" ", "").replaceAll("\"", "");
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
	}
	
	/**
	 * @param list 分析結果的陣列
	 * @param index 每個種類的ID
	 * @return 相對應ID的數量
	 */
	private static int getResultArray(List<Tuple2<String, Integer>> list , int index){
		for(int i=0 ;i<list.size();i++){
			Tuple2<String, Integer> value = list.get(i);
			String [] token = value._1().replaceAll(" ", "").split(":");
			if(token[1].equals(index+"")){
				return value._2();
			}
		}
		return 0;
	}

}
