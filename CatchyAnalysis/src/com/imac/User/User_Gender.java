package com.imac.User;

import java.sql.SQLException;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import com.imac.tag.CatchyTag;

public class User_Gender {
	private static JavaSparkContext sc ; 
	private static DataFrame dataFrame;
	private static SQLContext sqlContext;
	public User_Gender( JavaSparkContext sc, SQLContext sqlContext, DataFrame dataFrame){
		User_Gender.sc = sc;
		User_Gender.dataFrame = dataFrame;
		User_Gender.sqlContext = sqlContext;
	}
	
	public static void run() throws SQLException {

		dataFrame.registerTempTable("user_infos");
		
		DataFrame resultDataFrame = sqlContext.sql("SELECT * FROM (SELECT COUNT(*) AS boys FROM user_infos WHERE Gender = 0) AS boy" +
				", (SELECT COUNT(*) AS girls FROM user_infos WHERE Gender = 1) AS girl");
		resultDataFrame.printSchema();
		
		Row [] resultRow = resultDataFrame.collect();
		
		System.out.println("Boys Count : "+resultRow[0].get(0));
		System.out.println("Girls Count : "+resultRow[0].get(1));
		
		User.saveToMySQL(Integer.parseInt(resultRow[0].get(0).toString())
					   , Integer.parseInt(resultRow[0].get(1).toString())
					   , CatchyTag.TABLE6_1);
		
	}

}
