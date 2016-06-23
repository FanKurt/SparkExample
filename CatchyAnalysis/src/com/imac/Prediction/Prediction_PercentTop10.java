package com.imac.Prediction;

import java.sql.SQLException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;

import com.imac.tag.CatchyTag;

import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple4;

public class Prediction_PercentTop10 {
	private static JavaPairRDD<String, Map<String, Object>>  esData ;
	private static JavaSparkContext sc ; 
	private static DataFrame dataFrame;
	private static DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
	private static Calendar todayCalendar = Calendar.getInstance();
	private static Calendar weekCalendar = Calendar.getInstance();
	private static Calendar dataCalendar = Calendar.getInstance();
	private static String todayDate = "2016-03-18" ;
	public static final Pattern vote = Pattern.compile("/vote/(.*)()");
	private static final int LIMIT_VOTECOUNT = 20; //最低投票數量
	public Prediction_PercentTop10( JavaSparkContext sc, JavaPairRDD<String, 
			Map<String, Object>>  esData, DataFrame dataFrame) throws ParseException{
		this.sc = sc;
		this.esData = esData;
		this.dataFrame = dataFrame;
		this.todayDate = dateFormat.format(new Date()); // 今日
		setCalenderTime();
	}
	
	public static void run() throws SQLException {
		//正確答案 (文章ID,正確答案)
		final Map<String, String> voteMap = dataFrame.toJavaRDD().mapToPair(new PairFunction<Row, String, String>() {
			public Tuple2<String, String> call(Row arg0) throws Exception {
				String value = arg0.get(1)+","+arg0.get(2)+","+arg0.get(3);
				return new Tuple2<String, String>(arg0.get(0).toString(),selectCorrectAnswer(value));
			}
		}).collectAsMap();
		
		//資料格式整理
		JavaRDD<Tuple3<String, String,String>> messageRDD =esData.map(new Function<Tuple2<String,Map<String,Object>>, Tuple3<String, String,String>>() {
			public Tuple3<String, String,String> call(
					Tuple2<String, Map<String, Object>> arg0) throws Exception {
				try{
					String url = arg0._2().get("url").toString();
					String message = arg0._2().get("message").toString();
					String time = arg0._2.get("time").toString();
					String [] times = time.split(" ");
					return new Tuple3<String, String,String>(url, message, times[0]);
				}catch(Exception e){
					return null;
				}
			}
		}).filter(new Function<Tuple3<String, String,String>, Boolean>() {
			public Boolean call(Tuple3<String, String,String> arg0) throws Exception {
				return arg0!=null && arg0._1().contains("vote") && arg0._2().contains("Option");
			}
		});
		
		//使用者投票次數
		JavaPairRDD<String, Integer> voteCountRDD = messageRDD.mapToPair(new PairFunction<Tuple3<String, String,String>, String, Integer>() {
			public Tuple2<String, Integer> call(Tuple3<String, String,String> arg0) throws Exception {
				try{
					Matcher m = vote.matcher(arg0._1());
					if(m.find()){
						String pid = m.group(1); // 取得 pid 
						String [] split = getDataFormat(arg0._2()).split(",");
						//找 UID
						String uidValue = split[8];
						String[] uids = uidValue.split("UID");
						String UID = uids[1].substring(0, uids[1].length()-2);
						return new Tuple2<String, Integer>(UID, 1);
					}
				}catch(Exception e){
					return null;
				}
				return null;
			}
		}).filter(new Function<Tuple2<String,Integer>, Boolean>() {
			public Boolean call(Tuple2<String, Integer> arg0) throws Exception {
				return arg0!=null;
			}
		}).reduceByKey(new Function2<Integer, Integer, Integer>() {
			public Integer call(Integer arg0, Integer arg1) throws Exception {
				return arg0 + arg1;
			}
		}).filter(new Function<Tuple2<String,Integer>, Boolean>() {
			public Boolean call(Tuple2<String, Integer> arg0) throws Exception {
				return arg0._2() >=LIMIT_VOTECOUNT;
			}
		});
		
		//使用者投票數達最低票數的列表
		final Map<String, Integer> voteCountMap= voteCountRDD.collectAsMap();
		
		//使用者答對題數
		JavaPairRDD<String, Integer> correctRDD = messageRDD.mapToPair(new PairFunction<Tuple3<String, String,String>, String, Integer>() {
			public Tuple2<String, Integer> call(Tuple3<String, String,String> arg0) throws Exception {
				try{
					Matcher m = vote.matcher(arg0._1());
					if(m.find()){
						String pid = m.group(1); // 取得 pid 
						String [] split = getDataFormat(arg0._2()).split(",");
						//找 Option ID
						String optionValue = split[6];
						String[] options = optionValue.split("Option");
						String optionID = options[1];
						//找 UID
						String uidValue = split[8];
						String[] uids = uidValue.split("UID");
						String UID = uids[1].substring(0, uids[1].length()-2);
						//比對 Option ID 與 Map陣列中的答案是否相同
						if(optionID.equals(voteMap.get(pid))){
							return new Tuple2<String, Integer>(UID, 1);
						}
					}
				}catch(Exception e){
					return null;
				}
				return null;
			}
		}).filter(new Function<Tuple2<String,Integer>, Boolean>() {
			public Boolean call(Tuple2<String, Integer> arg0) throws Exception {
				return arg0!=null;
			}
		}).reduceByKey(new Function2<Integer, Integer, Integer>() {
			public Integer call(Integer arg0, Integer arg1) throws Exception {
				return arg0 + arg1;
			}
		});
		
		//計算預測準確比例
		JavaPairRDD<Double, String> resultRDD = correctRDD.mapToPair(new PairFunction<Tuple2<String,Integer>, String, Double>() {
			public Tuple2<String, Double> call(Tuple2<String, Integer> arg0)
					throws Exception {
				if(voteCountMap.containsKey(arg0._1())){
					// 準確比例 = 答對題數/投票次數
					double percent = ((double)arg0._2() / (double)voteCountMap.get(arg0._1()))*100;
					return new Tuple2<String, Double>(arg0._1(), percent);
				}
				return null;
			}
		}).filter(new Function<Tuple2<String,Double>, Boolean>() {
			public Boolean call(Tuple2<String, Double> arg0) throws Exception {
				return arg0!=null;
			}
		}).mapToPair(new PairFunction<Tuple2<String,Double>, Double, String>() {
			public Tuple2<Double, String> call(Tuple2<String, Double> arg0)
					throws Exception {
				return arg0.swap();
			}
		}).sortByKey(false);
		
		for(Tuple2<Double, String> result : resultRDD.take(10)){
			Prediction.saveToMySQL(result._2(),result._1().intValue(), CatchyTag.TABLE7_4);
			System.out.println(result);
		}
		
	}
	
	/**
	 * 去除空白、冒號和雙引號
	 */
	private static String getDataFormat(String message){
		return message.replace(" ", "").replaceAll("\"", "").replaceAll(":", "");
	}
	
	
	/**
	 * 
	 * @param dataDate 資料日期
	 * @return 是否包含在時間區間內
	 * @throws ParseException 
	 */
	private static Boolean isInTimeZone(String dataDate) throws ParseException{
		try{
			dataCalendar.setTime(dateFormat.parse(dataDate));
			return weekCalendar.before(dataCalendar) && todayCalendar.after(dataCalendar);
		}catch(Exception e){
			return false;
		}
	}
	
	/**
	 * 設定 今日時間 與 2週前時間
	 * @throws ParseException
	 */
	private void setCalenderTime() throws ParseException {
		todayCalendar.setTime(dateFormat.parse(todayDate));
		todayCalendar.add(Calendar.DATE, +1);
		System.out.println("today : "+todayCalendar.getTime());
		weekCalendar.setTime(dateFormat.parse(todayDate));
		weekCalendar.add(Calendar.DATE, -14);
		System.out.println("week : "+weekCalendar.getTime());
	}
	
	/**
	 * @param lines 資料庫中的 選項數量
	 * @return 最高數量的選項
	 */
	private static String selectCorrectAnswer(String lines){
		String [] numbers = lines.split(",");
		int init=0;
		for(String value : numbers){
			int number= Integer.parseInt(value);
			if(number > init){
				init = number;
			}
		}
		for(int i=0 ;i<numbers.length ;i++){
			int number= Integer.parseInt(numbers[i]);
			if(init==number){
				return i+"";
			}
		}
		return "0";
	}

}
