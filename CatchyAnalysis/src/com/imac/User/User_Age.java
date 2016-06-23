package com.imac.User;

import java.sql.SQLException;
import java.util.Calendar;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import com.imac.tag.CatchyTag;

import scala.Tuple2;

public class User_Age {
	private static JavaSparkContext sc ; 
	private static DataFrame dataFrame;
	private static SQLContext sqlContext;
	public User_Age( JavaSparkContext sc, SQLContext sqlContext, DataFrame dataFrame){
		User_Age.sc = sc;
		User_Age.dataFrame = dataFrame;
		User_Age.sqlContext = sqlContext;
	}
	public static void run() throws SQLException {

		dataFrame.registerTempTable("user_infos");
		
		DataFrame ageDataFrame =sqlContext.sql("SELECT Birthday from user_infos");
		ageDataFrame.printSchema();
		
		JavaRDD<Row> rawData = ageDataFrame.toJavaRDD();
		
		JavaRDD<Integer> ageRDD = rawData.filter(new Function<Row, Boolean>() {
			public Boolean call(Row arg0) throws Exception {
				return arg0.get(0)!=null;
			}
		}).map(new Function<Row, Integer>() {
			public Integer call(Row arg0) throws Exception {
				String line = arg0.get(0).toString();
				String [] split = line.split(" ");
				String [] dates = split[0].split("-");
				return getAge(dates[0], dates[1], dates[2]);
			}
		});
		
		JavaPairRDD<String, Integer> resultRDD = ageRDD.mapToPair(new PairFunction<Integer, String, Integer>() {
			public Tuple2<String, Integer> call(Integer arg0) throws Exception {
				return selectAgeRange(arg0);
			}
		}).reduceByKey(new Function2<Integer, Integer, Integer>() {
			public Integer call(Integer arg0, Integer arg1) throws Exception {
				return arg0 + arg1;
			}
		});
		
		
		List<Tuple2<String, Integer>> resultList = resultRDD.collect();
		
		User.saveToMySQL(getResultArray(resultList, "<20")
				, getResultArray(resultList, "20~25")
				, getResultArray(resultList, "25~30")
				, getResultArray(resultList, "30~35")
				, getResultArray(resultList, "35~40")
				, getResultArray(resultList, ">40")
				, CatchyTag.TABLE6_4);
	}
	
	/**
  	 * @param year 年 (西元), e.g 1991
	 * @param month 月
	 * @param day 日
	 * @return 年齡
	 */
	private static int getAge(String year, String month, String day) {
		Calendar calDOB = Calendar.getInstance();
		calDOB.set(Integer.parseInt(year), Integer.parseInt(month),
				Integer.parseInt(day));
		
		Calendar calNow = Calendar.getInstance();
		calNow.setTime(new java.util.Date());
		
		int ageYr = (calNow.get(Calendar.YEAR) - calDOB.get(Calendar.YEAR));
		int ageMo = (calNow.get(Calendar.MONTH) - calDOB.get(Calendar.MONTH));
		
		if (ageMo < 0) {
			ageYr--;
		}
		return ageYr;
	}
	
	/**
	 * @param age 年齡
	 * @return 年齡分布
	 */
	private static Tuple2<String, Integer> selectAgeRange(Integer age) {
		if(age<20){
			return new Tuple2<String, Integer>("<20",1);
		}else if(age>=20 && age<25){
			return new Tuple2<String, Integer>("20~25",1);
		}else if(age>=25 && age<30){
			return new Tuple2<String, Integer>("25~30",1);
		}else if(age>=30 && age<35){
			return new Tuple2<String, Integer>("30~35",1);
		}else if(age>=35 && age<40){
			return new Tuple2<String, Integer>("35~40",1);
		}else{
			return new Tuple2<String, Integer>(">40",1);
		}
	}

	/**
	 * @param list 分析結果的陣列
	 * @param index 每個種類的名稱
	 * @return 相對應名稱的數量
	 */
	private static int getResultArray(List<Tuple2<String, Integer>> list , String index){
		for(int i=0 ;i<list.size();i++){
			Tuple2<String, Integer> value = list.get(i);
			if(value._1().equals(index)){
				return value._2();
			}
		}
		return 0;
	}
}
