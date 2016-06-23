package com.imac.Engagement;

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

import scala.Tuple2;

public class Engagement {
	private static DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
	public static void main(String[] args) throws SQLException, ParseException {
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
	 *  1 : 累積like總數最高的文章
	 *  3 : 累積comment總則數最多的文章
	 *  4 : 累積被追蹤總數最高的用戶
	 *  6 : 當日/週/月新增like數最多的文章
	 *  7 : 當日/週/月新增comment數最多的文章
	 *  9 : 當日/週/月新增被追蹤總數最多的用戶
	 *  11 : 累積所有Post中，種類比: 哪裡買：問意見y/n：問意見which：買賣交易
	 *  12 : 當日新增Post中，種類比: 哪裡買：問意見y/n：問意見which：買賣交易
	 * @throws SQLException 
	 * @throws ParseException 
	 */
	public static void selectAnalysisType(JavaSparkContext sc, JavaPairRDD<String
				, Map<String, Object>> esRDD, String[] args) throws SQLException, ParseException{
		String type = args[0];
		if(type.equals("1")){
			Engagement_Like like = new Engagement_Like(sc, esRDD);
			like.runWeekly();
		}else if(type.equals("3")){
			Engagement_Commend commend = new Engagement_Commend(sc, esRDD);
			commend.runWeekly();
		}else if(type.equals("4")){
			Engagement_Follow follow = new Engagement_Follow(sc, esRDD);
			follow.runWeekly();
		}else if(type.equals("6")){
			if(args.length != 2){
				System.out.println("Error Format {PackageType} {DateType}");
				System.exit(0);
			}
			Engagement_LikeByDate like = new Engagement_LikeByDate(sc, esRDD , args[1]);
			like.run();
		}else if(type.equals("7")){
			if(args.length != 2){
				System.out.println("Error Format {PackageType} {DateType}");
				System.exit(0);
			}
			Engagement_CommendByDate commend = new Engagement_CommendByDate(sc, esRDD , args[1]);
			commend.run();
		}else if(type.equals("9")){
			if(args.length != 2){
				System.out.println("Error Format {PackageType} {DateType}");
				System.exit(0);
			}
			Engagement_FollowByDate follow = new Engagement_FollowByDate(sc, esRDD , args[1]);
			follow.run();
		}else if(type.equals("11")){
			Engagement_DocumentType document = new Engagement_DocumentType(sc, esRDD);
			document.run();
		}else if(type.equals("12")){
			Engagement_DocumentTypeByDate document = new Engagement_DocumentTypeByDate(sc, esRDD);
			document.run();
		}
	}
	/**
	 * For 1、3、4
	 * @param object 分析結果
	 * @param table 表格名稱
	 */
	public static void saveToMySQL(Object object , String table) throws SQLException{
		Connection mcConnect = null;
		PreparedStatement mStatement = null;
		try {
			mcConnect = DriverManager.getConnection(
					CatchyTag.MYSQL_URL, CatchyTag.MYSQL_USER, CatchyTag.MYSQL_PASSWORD);
			mStatement = mcConnect.prepareStatement(table);
			mStatement.setString(1, object.toString());
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
	/**
	 * For 6、7、9
	 * @param object 分析結果
	 * @param table 表格名稱
	 * @param type
	 */
	public static void saveToMySQL(Object object , String table , int type) throws SQLException{
		Connection mcConnect = null;
		PreparedStatement mStatement = null;
		try {
			mcConnect = DriverManager.getConnection(
					CatchyTag.MYSQL_URL, CatchyTag.MYSQL_USER, CatchyTag.MYSQL_PASSWORD);
			mStatement = mcConnect.prepareStatement(table);
			mStatement.setInt(1, type);
			mStatement.setString(2, object.toString());
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
	 * For 11、12
	 * @param object 分析結果
	 * @param table 表格名稱
	 * @param type
	 */
	public static void saveToMySQL(int buy ,int sell , int share, int boo, int select,String table) throws SQLException{
		Connection mcConnect = null;
		PreparedStatement mStatement = null;
		try {
			mcConnect = DriverManager.getConnection(
					CatchyTag.MYSQL_URL, CatchyTag.MYSQL_USER, CatchyTag.MYSQL_PASSWORD);
			mStatement = mcConnect.prepareStatement(table);
			mStatement.setInt(1, buy);
			mStatement.setInt(2, sell);
			mStatement.setInt(3, share);
			mStatement.setInt(4, boo);
			mStatement.setInt(5, select);
			mStatement.setString(6, dateFormat.format(new Date()).toString());
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
