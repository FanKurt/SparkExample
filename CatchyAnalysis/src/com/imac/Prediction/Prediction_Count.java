package com.imac.Prediction;

import java.sql.SQLException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.LinkedHashMap;
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
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.json.simple.JSONObject;
import scala.Tuple2;
import scala.Tuple3;
import com.imac.tag.CatchyTag;

public class Prediction_Count {
	private static JavaPairRDD<String, Map<String, Object>>  esData ;
	private static JavaSparkContext sc ; 
	private static DataFrame dataFrame;
	private static DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
	private static Calendar todayCalendar = Calendar.getInstance();
	private static Calendar weekCalendar = Calendar.getInstance();
	private static Calendar dataCalendar = Calendar.getInstance();
	private static String todayDate = "2016-03-18" ;
	public static final Pattern vote = Pattern.compile("/vote/(.*)()");
	public Prediction_Count( JavaSparkContext sc, JavaPairRDD<String, 
			Map<String, Object>>  esData, DataFrame dataFrame) throws ParseException{
		this.sc = sc;
		this.esData = esData;
		this.dataFrame = dataFrame;
		this.todayDate = dateFormat.format(new Date()); // ����
		setCalenderTime();
	}
	
	public static void run() throws SQLException {
		//���T���� (�峹ID,���T����)
		final Map<String, String> voteMap = dataFrame.toJavaRDD().mapToPair(new PairFunction<Row, String, String>() {
			public Tuple2<String, String> call(Row arg0) throws Exception {
				String value = arg0.get(1)+","+arg0.get(2)+","+arg0.get(3);
				return new Tuple2<String, String>(arg0.get(0).toString(),selectCorrectAnswer(value));
			}
		}).collectAsMap();
		
		
		//��Ʈ榡��z
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
		
		//�ϥΪ̵����D��
		JavaPairRDD<String, Object> resultRDD = messageRDD.mapToPair(new PairFunction<Tuple3<String, String,String>, String, Object>() {
			public Tuple2<String, Object> call(Tuple3<String, String,String> arg0) throws Exception {
				try{
					Matcher m = vote.matcher(arg0._1());
					if(m.find()){
						String pid = m.group(1); // ���o pid 
						//�� Option ID
						String [] split = getDataFormat(arg0._2()).split(",");
						String optionValue = split[6];
						String[] options = optionValue.split("Option");
						String optionID = options[1];
						//�� UID
						String uidValue = split[8];
						String[] uids = uidValue.split("UID");
						String UID = uids[1].substring(0, uids[1].length()-2);
						//��� Option ID �P Map�}�C�������׬O�_�ۦP
						if(optionID.equals(voteMap.get(pid))){
							return new Tuple2<String, Object>(UID, 1.0);
						}
					}
				}catch(Exception e){
					return null;
				}
				return null;
			}
		}).filter(new Function<Tuple2<String,Object>, Boolean>() {
			public Boolean call(Tuple2<String, Object> arg0) throws Exception {
				return arg0!=null;
			}
		}).reduceByKey(new Function2<Object, Object, Object>() {
			public Object call(Object arg0, Object arg1) throws Exception {
				return (double)arg0 + (double)arg1;
			}
		});
		
		resultRDD.cache();
		
		
		JavaRDD<Object> voteCount = resultRDD.values(); //���ﲼ��
		
		JavaDoubleRDD doubleRDD = new JavaDoubleRDD(voteCount.rdd());
		Double voteMax = doubleRDD.max(); //�̰�����
		Double voteMin = doubleRDD.min(); //�̧C����
		
		System.out.println("MAX  "+voteMax);
		System.out.println("MIN  "+voteMin);
		
		//�h���Ƥ�
		final List<Integer> distinctArrList =sc.parallelize(getRangeArray(voteMin, voteMax)).distinct().collect();
		
		Collections.sort(distinctArrList); // �Ƨ�
		
		
		// �έp�U�Ӱ϶��ƭȤ����ƶq
		JavaPairRDD<Integer, Integer>  finalResultRDD = resultRDD.mapToPair(new PairFunction<Tuple2<String,Object>, Integer, Integer>() {
			public Tuple2<Integer, Integer> call(Tuple2<String, Object> arg0)
					throws Exception {
				return new Tuple2<Integer, Integer>(selectInterval(distinctArrList, (double)arg0._2()),1);
			}
		}).reduceByKey(new Function2<Integer, Integer, Integer>() {
			public Integer call(Integer arg0, Integer arg1) throws Exception {
				return arg0 + arg1;
			}
		});
		
		// �C�Ӱ϶����ƶq (Key�϶��ƭ� �A Value�϶��ƭȤ����ƶq)
		List<Tuple2<Integer, Integer>> result  = finalResultRDD.collect(); 
		
		
		String resulJsonString = generateJSON(distinctArrList , result);
		
		System.out.println(resulJsonString);
		
		
		Prediction.saveToMySQL(resulJsonString, voteMin, voteMax, CatchyTag.TABLE7_1);
	}
	
	/**
	 * @param distinctArrList ������X�b�϶��}�C
	 * @param result �϶����ƶq�έp���G���}�C
	 * @return JSON�r��
	 */
	private static String generateJSON(List<Integer> distinctArrList, List<Tuple2<Integer, Integer>> result) {
		Map<Integer,Integer> map = new LinkedHashMap<Integer,Integer>();
		for(Integer value : distinctArrList){
			map.put(value, getResultArray(result, value));
		}
		return JSONObject.toJSONString(map);
	}

	/**
	 * �h���ťաB�_���M���޸�
	 */
	private static String getDataFormat(String message){
		return message.replace(" ", "").replaceAll("\"", "").replaceAll(":", "");
	}
	
	
	/**
	 * 
	 * @param dataDate ��Ƥ��
	 * @return �O�_�]�t�b�ɶ��϶���
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
	 * @param �]�w ����ɶ� �P 2�g�e�ɶ�
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
	 * @param lines ��Ʈw���� �ﶵ�ƶq
	 * @return �̰��ƶq���ﶵ
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
	
	/**
	 * @param min �̧C�����D��
	 * @param max �̰������D��
	 * @return ������X�b�϶��}�C
	 */
	private static ArrayList<Integer> getRangeArray(double min , double max){
		ArrayList<Integer> doubleArrayList = new ArrayList<>();
		double interval = (max-min)/20;
		double initNumber = min;
		for(int i=0 ; i<=20 ; i++){
			int number = (int)(initNumber); // �N�p��4��5�J�è����
			doubleArrayList.add(number);
			initNumber+=interval;
		}
		return doubleArrayList;
	}
	
	/**
	 * �P�_�ƭȦ���ذ϶��}�C��
	 * @param distinctArrList ������X�b�϶��}�C
	 * @param index �����D��
	 * @return �������϶�
	 */
	private static int selectInterval(List<Integer> distinctArrList , double index){
		int iterval = 0;
		for(int i=0; i<distinctArrList.size(); i++){
			if(i < distinctArrList.size()-1){
				iterval = distinctArrList.get(i);
				int value = distinctArrList.get(i);
				int nextValue = distinctArrList.get(i+1);
				if(index >= value && index < nextValue){
					return iterval;
				}
			}else{
				return iterval;
			}
		}
		return 0;
	}
	
	/**
	 * @param result ���R���G���}�C
	 * @param index �C�Ӻ������W��
	 * @return �۹����W�٪��ƶq
	 */
	private static int getResultArray(List<Tuple2<Integer, Integer>> result , Integer index){
		for(int i=0 ;i<result.size();i++){
			Tuple2<Integer, Integer> value = result.get(i);
			if(value._1() == index){
				return value._2();
			}
		}
		return 0;
	}

}
