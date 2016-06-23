package com.imac.Active;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.serializer.KryoSerializer;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;

import com.imac.tag.CatchyTag;

public class Active {
	private static DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
	public static void main(String[] args) throws SQLException {
		
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
	 * 1 : 當日活躍人次（ＤＡＵ） 當日活躍人數（ＤＡＵ-distinct）
	 * 3 : 每小時在線活躍人數
	 * 4 : 當月活躍人次（ＭＡＵ） 當月活躍人數（ＭＡＵ-distinct）
	 */
	private static void selectAnalysisType(JavaSparkContext sc,
			JavaPairRDD<String, Map<String, Object>> esRDD, String[] args) throws SQLException {
		if(args[0].equals("1")){
			Active_Daily dau_Daily = new Active_Daily(sc, esRDD);
			dau_Daily.runDaily();
		}else if(args[0].equals("3")){
			Active_Hourly dau_Hourly = new Active_Hourly(sc, esRDD);
			dau_Hourly.runHourly();
		}else if(args[0].equals("4")){
			Active_Monthly dau_Monthly = new Active_Monthly(sc, esRDD);
			dau_Monthly.runMonthly();
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
