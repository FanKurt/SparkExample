package com.imac.Controller;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.tree.DecisionTree;
import org.apache.spark.mllib.tree.model.DecisionTreeModel;

import scala.Tuple2;

import com.imac.Etu;
import com.imac.Feature.TimeFeature;

public class EtuContoller {
	/**
	 * 將CSV檔案第一行標題去除
	 */
	public static JavaRDD<String> getDataFilter(JavaRDD<String> orderRDD){
		JavaRDD<String> filterRDD = orderRDD.filter(new Function<String, Boolean>() {
			public Boolean call(String arg0) throws Exception {
				String [] split = arg0.split(",");
				return !arg0.contains("userid");
			}
		});
		return filterRDD;
	}
	
	
	/**
	 * 資料特徵化
	 * @param etu 
	 * @param filterRDD 來源資料
	 * @param userConsumeList 
	 * @param userList 
	 * @param viewMap 
	 * @param viewMap 使用者瀏覽店家次數
	 * @param userList 使用者數值特徵對應表
	 * @param userConsumeList 使用者消費能力表
	 * @return
	 */
	public static JavaRDD<LabeledPoint> getDataFeature(JavaRDD<String> filterRDD
			, final Map<String, Double> viewMap
			, final List<Tuple2<String, Long>> userList
			, final Map<String, Double> userConsumeList){
		
		JavaRDD<LabeledPoint> rawRDD =filterRDD.map(new Function<String, LabeledPoint>() {
			public LabeledPoint call(String arg0) throws Exception {
				String [] split = arg0.split(",");
				return new LabeledPoint(Double.parseDouble(split[0]), Vectors.dense(getFeature(split)));
			}
			/**
			 * @param split 特徵屬性陣列
			 * @return 數值特徵陣列
			 * @throws NumberFormatException
			 * @throws ParseException
			 */
			private double [] getFeature(String[] split) throws NumberFormatException, ParseException{
				double[] features = new double[13];
				features[0]= Double.parseDouble(split[1]); //catid_1
				features[1]= Double.parseDouble(split[2]); //catid_2
				features[2]= Double.parseDouble(split[3]); //orderno
				features[3]= Double.parseDouble(split[5]); //amt
				//使用者瀏覽店家次數
				try{
					features[4]= viewMap.get(split[0]+","+split[split.length-1]);
				}catch(Exception e){
					features[4]= 0.0;
				}
				
				String userId = split[split.length-1];
				for(Tuple2<String, Long> value : userList){
					if(value._1().equals(userId)){
						features[5]= (double)value._2(); //uerid
					}
				}
				
				features[6]= Double.parseDouble(TimeFeature.getMinute(split[4])); //date
				features[7]= Double.parseDouble(TimeFeature.getDayOfMonth(split[4])); //date
				
				if(userConsumeList.containsKey(userId)){
					features[8]= userConsumeList.get(userId);
				}else{
					features[8]= 0.0;
				}
			
				features[9]= Double.parseDouble(TimeFeature.getHour(split[4])); //date
				features[10]= Double.parseDouble(TimeFeature.getWeekOfDay(split[4])); //date
				features[11]= Double.parseDouble(TimeFeature.getSecond(split[4])); //date
				features[12]= Double.parseDouble(TimeFeature.getMonth(split[4])); //date
				return features;
			}
		});
		return rawRDD;
	}
	
	
	/**
	 * 決策樹模型訓練
	 * @param trainingRDD 訓練資料RDD
	 * @return
	 */
	public static DecisionTreeModel getDecisionModel(JavaRDD<LabeledPoint> trainingRDD){
		Integer numClasses = 14;
	    Map<Integer, Integer> categoricalFeaturesInfo = new HashMap<>();
	    String impurity = "gini";
	    Integer maxDepth = 15;
	    Integer maxBins = 1024;
	    DecisionTreeModel model = DecisionTree.trainClassifier(trainingRDD, numClasses,
	      categoricalFeaturesInfo, impurity, maxDepth, maxBins);
		return model;
	}
	
	/**
	 * 決策樹模型測試
	 * @param testingRDD 測試資料RDD
	 * @param model 決策樹模型
	 * @return
	 */
	public static JavaPairRDD<Object, Object> getModelTesting(JavaRDD<LabeledPoint> testingRDD, final DecisionTreeModel model){
		JavaPairRDD<Object, Object> labelAndPrdict = testingRDD.mapToPair(new PairFunction<LabeledPoint, Object, Object>() {
			public Tuple2<Object, Object> call(LabeledPoint arg0)throws Exception {
				try{
					System.out.println(arg0.features());
					return new Tuple2<Object, Object>(model.predict(arg0.features()) , arg0.label());
				}catch(Exception e){
					return null;
				}
			}
		}).filter(new Function<Tuple2<Object,Object>, Boolean>() {
			public Boolean call(Tuple2<Object, Object> arg0) throws Exception {
				return arg0!=null;
			}
		});
		return labelAndPrdict;
	}
	
	
}
