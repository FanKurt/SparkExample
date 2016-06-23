package com.imac.Prediction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.serializer.KryoSerializer;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;

import com.imac.tag.CatchyTag;

public class Prediction {
	private static DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
	
	public static void main(String[] args) throws SQLException, ParseException {
		
		if(args.length!=1){
			System.out.println("Format ERROR , {Type}");
			System.exit(0);
		}
		
		SparkConf conf = new SparkConf().setAppName("CatchyAnalysis");
		conf.set("spark.serializer", KryoSerializer.class.getName());
		conf.set("es.index.auto.create", "true");
		conf.set("es.nodes", "10.26.1.53:9200");
		conf.set("es.resource", "catchy-server/logs");
		conf.set("es.input.json" , "true");
		
		JavaSparkContext sc = new JavaSparkContext(conf);
		SQLContext sqlContext = new SQLContext(sc);
		
		JavaPairRDD<String, Map<String, Object>> esRDD = JavaEsSpark.esRDD(sc);
		
		DataFrame dataFrame = sqlContext.read().format("jdbc").options(getOptions()).load();
		
		selectAnalysisType(sc ,sqlContext ,esRDD,dataFrame, args);
		
		sc.stop();
	}
	
	/**
	 * @param sqlContext 
	 * @param dataFrame 
	 * @param args
	 * 1 : 預測準確數量
	 * 2 : 預測準確比例
	 * 3 : 準確數量最多用戶
	 * 4 : 準確比例最多用戶
	 * @throws ParseException 
	 */
	private static void selectAnalysisType(JavaSparkContext sc,
			SQLContext sqlContext, JavaPairRDD<String, Map<String, Object>> esRDD
			, DataFrame dataFrame, String[] args) throws SQLException, ParseException {
		
		if(args[0].equals("1")){
			Prediction_Count count = new Prediction_Count(sc,esRDD,dataFrame);
			count.run();
		}else if(args[0].equals("2")){
			Prediction_Percent percent = new Prediction_Percent(sc,esRDD,dataFrame);
			percent.run();
		}else if(args[0].equals("3")){
			Prediction_CountTop10 count = new Prediction_CountTop10(sc,esRDD,dataFrame);
			count.run();
		}else if(args[0].equals("4")){
			Prediction_PercentTop10 percent = new Prediction_PercentTop10(sc,esRDD,dataFrame);
			percent.run();
		}
		
	}
	
	/**
	 * 設定抓取的資料表名稱和資料欄位
	 */
	private static Map<String, String> getOptions(){
		Map<String, String> options = new HashMap<>();
		options.put("driver", CatchyTag.MYSQL_DRIVER);
		options.put("url", CatchyTag.MYSQL_CONNECTION_URL);
		options.put("partitionColumn", "PID");
		options.put("lowerBound", "0");
		options.put("upperBound", "10");
		options.put("numPartitions", "10");
		options.put("dbtable","(select * from product_vote_nums) as catchy_vote");
		return options;
	}

	/**
	 * For 3、4
	 * @param user 使用者ID
	 * @param count 數量/比例
	 * @param table 資料表
	 * @throws SQLException
	 */
	public static void saveToMySQL(String user , int count , String table) throws SQLException{
		Connection mcConnect = null;
		PreparedStatement mStatement = null;
		try {
			mcConnect = DriverManager.getConnection(
					CatchyTag.MYSQL_URL, CatchyTag.MYSQL_USER, CatchyTag.MYSQL_PASSWORD);
			mStatement = mcConnect.prepareStatement(table);
			mStatement.setString(1, user);
			mStatement.setInt(2, count);
			mStatement.setString(3, dateFormat.format(new Date()).toString());
			mStatement.executeUpdate();
		} 
		finally {
			if (mcConnect != null) {
				mcConnect.close();
			}
			if (mStatement != null) {
				mStatement.close();
			}
		}
	}
	/**
	 * For 1
	 * @param json 長條圖的數值資訊
	 * @param min 長條圖 X 軸最小值
	 * @param max 長條圖 X 軸最大值
	 * @param table 資料表
	 * @throws SQLException
	 */
	public static void saveToMySQL(String json , double min , double max , String table) throws SQLException{
		Connection mcConnect = null;
		PreparedStatement mStatement = null;
		try {
			mcConnect = DriverManager.getConnection(
					CatchyTag.MYSQL_URL, CatchyTag.MYSQL_USER, CatchyTag.MYSQL_PASSWORD);
			mStatement = mcConnect.prepareStatement(table);
			mStatement.setString(1, json);
			mStatement.setFloat(2, (float) min);
			mStatement.setFloat(3, (float) max);
			mStatement.setString(4, dateFormat.format(new Date()).toString());
			mStatement.executeUpdate();
		} 
		finally {
			if (mcConnect != null) {
				mcConnect.close();
			}
			if (mStatement != null) {
				mStatement.close();
			}
		}
	}
	
	/**
	 * For2
	 * @param json 長條圖的數值資訊
	 * @param table 資料表
	 * @throws SQLException
	 */
	public static void saveToMySQL(String json, String table) throws SQLException{
		Connection mcConnect = null;
		PreparedStatement mStatement = null;
		try {
			mcConnect = DriverManager.getConnection(
					CatchyTag.MYSQL_URL, CatchyTag.MYSQL_USER, CatchyTag.MYSQL_PASSWORD);
			mStatement = mcConnect.prepareStatement(table);
			mStatement.setString(1, json);
			mStatement.setString(2, dateFormat.format(new Date()).toString());
			mStatement.executeUpdate();
		} 
		finally {
			if (mcConnect != null) {
				mcConnect.close();
			}
			if (mStatement != null) {
				mStatement.close();
			}
		}
	}

}
