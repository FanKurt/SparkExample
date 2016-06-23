package com.imac.Retention;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.serializer.KryoSerializer;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;

import com.imac.tag.CatchyTag;

public class Retention {
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
		
		JavaPairRDD<String, Map<String, Object>> esRDD = JavaEsSpark.esRDD(sc);
		
		selectAnalysisType(sc , esRDD , args);
		
		sc.stop();
	}
	
	/**
	 * @param args
	 * 1 : 本日retain人數
	 * 2 : 當月retain人數
	 * 3 : 本週retain人數
	 * @throws ParseException 
	 */
	private static void selectAnalysisType(JavaSparkContext sc,
			JavaPairRDD<String, Map<String, Object>> esRDD, String[] args) throws SQLException, ParseException {
		if(args[0].equals("1")){
			Retention_Daily retain_Daily = new Retention_Daily(sc,esRDD);
			retain_Daily.runDaily();
		}else if(args[0].equals("2")){
			Retention_Monthly retain_Monthly = new Retention_Monthly(sc, esRDD);
			retain_Monthly.runMonthly();
		}else if (args[0].equals("3")){
			Retention_Weekly retain_Weekly = new Retention_Weekly(sc, esRDD);
			retain_Weekly.runWeekly();
		}
	}

	public static void saveToMySQL(Object object , String table) throws SQLException{
		Connection mcConnect = null;
		PreparedStatement mStatement = null;
		try {
			mcConnect = DriverManager.getConnection(
					CatchyTag.MYSQL_URL, CatchyTag.MYSQL_USER, CatchyTag.MYSQL_PASSWORD);
			mStatement = mcConnect.prepareStatement(table);
			mStatement.setInt(1, Integer.parseInt(object+""));
			mStatement.setString(2, dateFormat.format(new Date()).toString());
			mStatement.executeUpdate();
			System.out.println("SQL in");
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
