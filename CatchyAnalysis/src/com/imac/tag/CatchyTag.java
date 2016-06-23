package com.imac.tag;
public class CatchyTag {
	/**
	 * Spark於MySQL儲存
	 */
	public static final String MYSQL_NAME="catchy";
	public static final String MYSQL_IP="211.23.17.100";
	public static final String MYSQL_USER="catchy";
	public static final String MYSQL_PASSWORD="aIf4RbnOeGcrhAdIaDqokovi2";
	public static final String MYSQL_URL="jdbc:mysql://"+MYSQL_IP+"/"+MYSQL_NAME;
	
	/**
	 * SparkSQL存取MySQL
	 */
	public static final String MYSQL_DRIVER = "com.mysql.jdbc.Driver";
	public static final String MYSQL_CONNECTION_URL = "jdbc:mysql://"+MYSQL_IP+":3306/"+MYSQL_NAME+
														"?zeroDateTimeBehavior=convertToNull" +
            											"&user=" + MYSQL_USER + "&password=" + MYSQL_PASSWORD;

	/**
	 * MySQL儲存資料表
	 */
	public static String TABLE2_1="insert into `analysis_2-1`(daily,time) values(?,?)";
	public static String TABLE2_2="insert into `analysis_2-2`(monthly,time) values(?,?)";
	public static String TABLE2_3="insert into `analysis_2-3`(weekly,time) values(?,?)";
	public static String TABLE3_1="insert into `analysis_3-1`(daily,time) values(?,?)";
//	public static String TABLE3_2="insert into `analysis_3-2`(daily,time) values(?,?)";
	public static String TABLE3_3="insert into `analysis_3-3`(hourly,time) values(?,?)";
	public static String TABLE3_4="insert into `analysis_3-4`(monthly,time) values(?,?)";
//	public static String TABLE3_5="insert into `analysis_3-5`(monthly,time) values(?,?)";
	public static String TABLE5_1="insert into `analysis_5-1`(weekly,time) values(?,?)";
	public static String TABLE5_3="insert into `analysis_5-3`(weekly,time) values(?,?)";
	public static String TABLE5_4="insert into `analysis_5-4`(weekly,time) values(?,?)";
	public static String TABLE5_6="insert into `analysis_5-6`(type,daily,time) values(?,?,?)";
	public static String TABLE5_7="insert into `analysis_5-7`(type,daily,time) values(?,?,?)";
	public static String TABLE5_9="insert into `analysis_5-9`(type,daily,time) values(?,?,?)";
	public static String TABLE5_11="insert into `analysis_5-11`(buy,sell,share,boolean,`select`,time) values(?,?,?,?,?,?)";
	public static String TABLE5_12="insert into `analysis_5-12`(buy,sell,share,boolean,`select`,time) values(?,?,?,?,?,?)";
	public static String TABLE6_1="insert into `analysis_6-1`(boys,girls,time) values(?,?,?)";
	public static String TABLE6_2="insert into `analysis_6-2`(fb,`non_fb`,time) values(?,?,?)";
	public static String TABLE6_4="insert into `analysis_6-4`(`<20`,`20-25`,`25-30`,`30-35`,`35-40`,`>40`,time) values(?,?,?,?,?,?,?)";
	
	public static String TABLE7_1="insert into `analysis_7-1`(daily,min,max,time) values(?,?,?,?)";
	public static String TABLE7_2="insert into `analysis_7-2`(daily,time) values(?,?)";
	public static String TABLE7_3="insert into `analysis_7-3`(user,daily,time) values(?,?,?)";
	public static String TABLE7_4="insert into `analysis_7-4`(user,daily,time) values(?,?,?)";
	
	public static String TABLE8_1="insert into `analysis_8-1`(weekly,min,max,time) values(?,?,?,?)";
	public static String TABLE8_2="insert into `analysis_8-2`(weekly,min,max,time) values(?,?,?,?)";

}
