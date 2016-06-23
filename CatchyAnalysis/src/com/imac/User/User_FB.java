package com.imac.User;

import java.sql.SQLException;
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

public class User_FB {
	private static JavaSparkContext sc ; 
	private static DataFrame dataFrame;
	private static SQLContext sqlContext;
	public User_FB( JavaSparkContext sc, SQLContext sqlContext, DataFrame dataFrame){
		User_FB.sc = sc;
		User_FB.dataFrame = dataFrame;
		User_FB.sqlContext = sqlContext;
	}
	
	public static void run() throws SQLException {

		dataFrame.registerTempTable("users");
		
		DataFrame ageDataFrame =sqlContext.sql("SELECT Account from users");
		ageDataFrame.printSchema();
		
		JavaRDD<Row> rawData = ageDataFrame.toJavaRDD();
		
		JavaPairRDD<String, Integer> resultRDD = rawData.mapToPair(new PairFunction<Row, String, Integer>() {
			public Tuple2<String, Integer> call(Row arg0) throws Exception {
				String account = arg0.get(0).toString();
				if(account.contains("@facebook.com")){
					return new Tuple2<String, Integer>("FB", 1);
				}else{
					return new Tuple2<String, Integer>("NotFB", 1);
				}
			}
		}).reduceByKey(new Function2<Integer, Integer, Integer>() {
			public Integer call(Integer arg0, Integer arg1) throws Exception {
				return arg0 + arg1;
			}
		});
		
		
		List<Tuple2<String, Integer>> resultList = resultRDD.collect();
		
		User.saveToMySQL(getResultArray(resultList, "FB")
						, getResultArray(resultList, "NotFB")
						, CatchyTag.TABLE6_2);
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
