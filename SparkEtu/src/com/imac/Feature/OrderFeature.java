package com.imac.Feature;

import java.util.List;
import java.util.Map;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class OrderFeature {
	public static List<Tuple2<String, Long>> getUserNumerical(JavaRDD<String> filterRDD){
		
		List<Tuple2<String, Long>>  userList = filterRDD.map(new Function<String, String>() {
			public String call(String arg0) throws Exception {
				String [] split = arg0.split(",");
				return split[split.length-1];
			}
		}).distinct().zipWithIndex().collect();
		return userList;
	}
	
	public static Map<String, Double> getUserConsumeLevel(JavaRDD<String> filterRDD){
		return filterRDD.mapToPair(new PairFunction<String, String, Integer>() {
			public Tuple2<String, Integer> call(String arg0) throws Exception {
				String [] split = arg0.split(",");
				return new Tuple2<String, Integer>(split[split.length-1], Integer.parseInt(split[5]));
			}
		}).reduceByKey(new Function2<Integer, Integer, Integer>() {
			public Integer call(Integer arg0, Integer arg1) throws Exception {
				return arg0+arg1;
			}
		}).mapToPair(new PairFunction<Tuple2<String,Integer>, String, Double>() {
			public Tuple2<String, Double> call(Tuple2<String, Integer> arg0)
					throws Exception {
				return new Tuple2<String, Double>(arg0._1(),selectConsumeLevel(arg0._2()));
			}
			/**
			 * 依據 5% 10% 25% 50% 75% 90% 95%來區分消費者消費能力
			 * @param integer
			 * @return 
			 */
			private Double selectConsumeLevel(Integer integer){
				int total = integer.intValue();
				if(total <=152){
					return 0.0; 
				}else if(total >152 && total <= 221){
					return 1.0; 
				}else if(total >221 && total <= 399){
					return 2.0; 
				}else if(total >399 && total <= 895){
					return 3.0; 
				}else if(total >895 && total <= 1699){
					return 4.0; 
				}else if(total >1699 && total <= 3300){
					return 5.0; 
				}else if(total >3300 && total <= 5490){
					return 6.0; 
				}else{
					return 7.0; 
				}
			}
		}).collectAsMap();
	}
}
