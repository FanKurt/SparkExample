package com.imac;

import java.text.ParseException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.spark.Accumulator;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.mllib.evaluation.MulticlassMetrics;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.tree.model.DecisionTreeModel;
import org.apache.spark.mllib.util.MLUtils;
import org.apache.spark.rdd.RDD;

import scala.Tuple2;
import scala.collection.generic.BitOperations.Int;

import com.imac.Controller.EtuContoller;
import com.imac.Feature.OrderFeature;
import com.imac.Feature.TimeFeature;
import com.imac.Feature.ViewFeature;

public class Etu {
	 private static Map<String, Double> viewMap;
	 private static List<Tuple2<String, Long>> userList;
	 private static Map<String, Double> userConsumeList;
	 
	 public static void main(String[] args) {
		if (args.length != 2) {
			System.out.println("Format Error : {source1} {source2} {output}");
			System.exit(0);
		}
		
		String sourcePath1 = args[0];
		String sourcePath2 = args[1];

		JavaSparkContext sc = new JavaSparkContext();

		JavaRDD<String> viewRDD = sc.textFile(sourcePath1);
		JavaRDD<String> orderRDD = sc.textFile(sourcePath2);
		
		
		//去除csv第一行標題
		JavaRDD<String> filterRDD = EtuContoller.getDataFilter(orderRDD);
		
		JavaRDD<String> trainRDD = filterRDD.filter(new Function<String, Boolean>() {
			public Boolean call(String arg0) throws Exception {
				return TimeFeature.isTrainingTime(arg0);
			}
		});
		
		//使用者瀏覽次數
		Map<String, Double> viewMap = ViewFeature.getUserShopScan(viewRDD);
		
		
		//使用者ID轉數值特徵
		List<Tuple2<String, Long>> userList = OrderFeature.getUserNumerical(trainRDD);
		
		
		//消費者消費能力
		Map<String, Double> userConsumeList = OrderFeature.getUserConsumeLevel(trainRDD);
		
		
		//資料特徵化
		JavaRDD<LabeledPoint> rawRDD = EtuContoller.getDataFeature(trainRDD,viewMap,userList,userConsumeList);
		
		//決策樹模型訓練
		final DecisionTreeModel model = EtuContoller.getDecisionModel(rawRDD);
		
		
		
		JavaRDD<String> testRDD = filterRDD.filter(new Function<String, Boolean>() {
			public Boolean call(String arg0) throws Exception {
				return TimeFeature.isTestingTime(arg0);
			}
		});
		
	
		// 測試資料預測
		JavaPairRDD<Object, Object> labelAndPrdict = EtuContoller.getModelTesting(test(testRDD), model);
		
//		
		double accuracy = labelAndPrdict.filter(new Function<Tuple2<Object,Object>, Boolean>() {
			public Boolean call(Tuple2<Object, Object> arg0) throws Exception {
				return (double)arg0._1() == (double)arg0._2();
			}
		}).count() / (double) labelAndPrdict.count();
		
		// Get evaluation metrics.
		MulticlassMetrics metrics = new MulticlassMetrics(labelAndPrdict.rdd());
		
		// Overall statistics
		System.out.println("Accuracy = " + accuracy);
		System.out.println("Precision = " + metrics.precision());
		System.out.println("Recall = " + metrics.recall());
		System.out.println("F1 Score = " + metrics.fMeasure());
	
//		Accumulator<Double> precision = sc.accumulator(0.0);
//		Accumulator<Double> recall = sc.accumulator(0.0);
//		Accumulator<Double> f1_score = sc.accumulator(0.0);
//		Accumulator<Integer> time = sc.accumulator(0);
		
		
		//十折交叉驗證
//		Tuple2<RDD<LabeledPoint>, RDD<LabeledPoint>>[] k_fold = MLUtils.kFold(
//				rawRDD.rdd(), 10, 11, rawRDD.classTag());
//
//		for (int i = 0; i < k_fold.length; i++) {
//
//			long start = System.currentTimeMillis();
//
//			JavaRDD<LabeledPoint> trainingRDD = k_fold[i]._1().toJavaRDD()
//					.cache();
//			JavaRDD<LabeledPoint> testingRDD = k_fold[i]._2().toJavaRDD()
//					.cache();
//			
//			//決策樹模型訓練
//			final DecisionTreeModel model = EtuContoller
//					.getDecisionModel(trainingRDD);
//
//			// 測試資料預測
//			JavaPairRDD<Object, Object> labelAndPrdict = EtuContoller.getModelTesting(testingRDD, model);
//
//			for (Tuple2<Object, Object> v : labelAndPrdict.take(20)) {
//				System.out.println(v);
//			}
//			
//			long end = System.currentTimeMillis();
//
//			
//			double accuracy = labelAndPrdict.filter(new Function<Tuple2<Object,Object>, Boolean>() {
//				public Boolean call(Tuple2<Object, Object> arg0) throws Exception {
//					return (double)arg0._1() == (double)arg0._2();
//				}
//			}).count() / (double) labelAndPrdict.count();
//			// Get evaluation metrics.
//			MulticlassMetrics metrics = new MulticlassMetrics(labelAndPrdict.rdd());
//			
//			// Overall statistics
//			System.out.println("Accuracy = " + accuracy);
//			System.out.println("Precision = " + metrics.precision());
//			System.out.println("Recall = " + metrics.recall());
//			System.out.println("F1 Score = " + metrics.fMeasure());
//
//			System.out.println((end - start) / 1000 + "  s");
//
//			long v = ((end - start) / 1000);
//
//			time.add((int) v);
//			precision.add(metrics.precision());
//			recall.add(metrics.recall());
//			f1_score.add(metrics.fMeasure());
//		}
//
//		System.out.println("Average Precision = " + precision.value()/ k_fold.length);
//		System.out.println("Average Recall = " + recall.value() / k_fold.length);
//		System.out.println("Average F1 Score = " + f1_score.value()/ k_fold.length);
//		System.out.println(time.value() / k_fold.length + " s");
		
		sc.stop();
	}

	private static JavaRDD<LabeledPoint> test(JavaRDD<String> filterRDD){
		//資料特徵化
		JavaRDD<LabeledPoint> rawRDD =filterRDD.map(new Function<String, LabeledPoint>() {
			public LabeledPoint call(String arg0) throws Exception {
				String [] split = arg0.split(",");
				return new LabeledPoint(Double.parseDouble(split[0]), Vectors.dense(getFeature(split)));
			}
			/**
			 * @return 時間特徵陣列
			 */
			private double [] getFeature(String[] split) throws NumberFormatException, ParseException{
				double[] features = new double[6];
				features[0]= Double.parseDouble(TimeFeature.getMinute(split[4])); //date
				features[1]= Double.parseDouble(TimeFeature.getDayOfMonth(split[4])); //date
				features[2]= Double.parseDouble(TimeFeature.getHour(split[4])); //date
				features[3]= Double.parseDouble(TimeFeature.getWeekOfDay(split[4])); //date
				features[4]= Double.parseDouble(TimeFeature.getSecond(split[4])); //date
				features[5]= Double.parseDouble(TimeFeature.getMonth(split[4])); //date
				return features;
			}
		});
		
		for(LabeledPoint v :rawRDD.take(10)){
			System.out.println(v);
		}
	
		return rawRDD;
	}
	
//	public List<Tuple2<String, Long>> getUserList(){
//		return userList;	
//	}
//	
//	public Map<String, Double> getUserConsumeList(){
//		return userConsumeList;
//	}
//	
//	public Map<String, Double> getViewMap(){
//		return viewMap;
//	}


}
