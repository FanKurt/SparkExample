package com.imac.User;

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

public class User {
	private static DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");

	public static void main(String[] args) throws SQLException {
		
		if (args.length != 1) {
			System.out.println("Format ERROR , {Type}");
			System.exit(0);
		}
		
		JavaSparkContext sc = new JavaSparkContext();
		SQLContext sqlContext = new SQLContext(sc);
		
		selectAnalysisType(sc, sqlContext, args);
		sc.stop();
	}
	/**
	 * @param args
	 * 1 : 所有用戶男：女
	 * 2 : 所有用戶FB : non-FB帳號
	 * 4 : 所有註冊用戶歲數分佈
	 * @throws ParseException 
	 */
	private static void selectAnalysisType(JavaSparkContext sc,SQLContext sqlContext, String[] args)
			throws SQLException {
		DataFrame dataFrame = sqlContext.read().format("jdbc").options(getOptions(args[0])).load();
		if (args[0].equals("1")) {
			User_Gender gender = new User_Gender(sc,sqlContext,dataFrame);
			gender.run();
		}else if(args[0].equals("2")){
			User_FB fb = new User_FB(sc,sqlContext,dataFrame);
			fb.run();
		}
		else if(args[0].equals("4")){
			User_Age age = new User_Age(sc,sqlContext,dataFrame);
			age.run();
		}
	}
	/**
	 * 設定抓取的資料表名稱和資料欄位
	 * @param type  資料表名稱  (1、4 :user_infos)  (2:users)
	 */
	private static Map<String, String> getOptions(String type){
		Map<String, String> options = new HashMap<>();
		options.put("driver", CatchyTag.MYSQL_DRIVER);
		options.put("url", CatchyTag.MYSQL_CONNECTION_URL);
		options.put("partitionColumn", "UID");
		options.put("lowerBound", "0");
		options.put("upperBound", "10");
		options.put("numPartitions", "10");
		
		if(type.equals("2")){
			options.put("dbtable","(select UID,Account from users) as catchy_user");
		}else{
			options.put("dbtable","(select UID,Birthday,Gender from user_infos) as catchy_user");
		}
		return options;
	}

	/*
	 * For 1、4
	 */
	public static void saveToMySQL(int number1 , int number2, String table)
			throws SQLException {
		Connection mcConnect = null;
		PreparedStatement mStatement = null;
		try {
			mcConnect = DriverManager.getConnection(CatchyTag.MYSQL_URL,
					CatchyTag.MYSQL_USER, CatchyTag.MYSQL_PASSWORD);
			mStatement = mcConnect.prepareStatement(table);
			mStatement.setInt(1, number1);
			mStatement.setInt(2, number2);
			mStatement.setString(3, dateFormat.format(new Date()).toString());
			mStatement.executeUpdate();
		} finally {
			if (mcConnect != null) {
				mcConnect.close();
			}
			if (mStatement != null) {
				mStatement.close();
			}
		}
	}
	
	/**
	 * @param number1 <20
	 * @param number2 20~25
	 * @param number3 25~30
	 * @param number4 30~35
	 * @param number5 35~40
	 * @param number6 >40
	 * @param table 資料表名稱
	 * @throws SQLException
	 */
	public static void saveToMySQL(int number1 , int number2, int number3, int number4
								, int number5,int number6, String table)throws SQLException {
		Connection mcConnect = null;
		PreparedStatement mStatement = null;
		try {
			mcConnect = DriverManager.getConnection(CatchyTag.MYSQL_URL,
					CatchyTag.MYSQL_USER, CatchyTag.MYSQL_PASSWORD);
			mStatement = mcConnect.prepareStatement(table);
			mStatement.setInt(1, number1);
			mStatement.setInt(2, number2);
			mStatement.setInt(3, number3);
			mStatement.setInt(4, number4);
			mStatement.setInt(5, number5);
			mStatement.setInt(6, number6);
			mStatement.setString(7, dateFormat.format(new Date()).toString());
			mStatement.executeUpdate();
		} finally {
			if (mcConnect != null) {
				mcConnect.close();
			}
			if (mStatement != null) {
				mStatement.close();
			}
		}
	}

}
