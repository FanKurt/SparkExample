package com.imac.Feature;

import java.util.Map;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;


public class ViewFeature {
	/**
	 * @param viewRDD
	 * @return 使用者瀏覽店家次數陣列
	 */
	public static Map<String, Double> getUserShopScan(JavaRDD<String> viewRDD){
		Map<String, Double>  viewMap = viewRDD.filter(new Function<String, Boolean>() {
			public Boolean call(String arg0) throws Exception {
				return !arg0.contains("userid");
			}
		}).mapToPair(new PairFunction<String, String, Double>() {
			public Tuple2<String, Double> call(String arg0) throws Exception {
				String [] split = arg0.split(",");
				String uid = split[2]+","+split[split.length-1];
				return new Tuple2<String, Double>(uid,1.0);
			}
		}).reduceByKey(new Function2<Double, Double, Double>() {
			public Double call(Double arg0, Double arg1) throws Exception {
				return arg0 + arg1;
			}
		}).collectAsMap();
		return viewMap;
	}
	
	
	
}
