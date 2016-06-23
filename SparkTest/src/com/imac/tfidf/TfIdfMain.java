package com.imac.tfidf;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;

import org.apache.spark.Accumulator;
import org.apache.spark.Partition;
import org.apache.spark.Partitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.serializer.KryoSerializer;
import org.apache.spark.storage.StorageLevel;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;
import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

import scala.Tuple2;

public class TfIdfMain {
//	private static int uuid = 0;
	private static String lastTerm = "";
	private static int numberOfDocuments, docCountForTerm = 1;

	public static void main(String[] args) {

		String inputpath = args[0];
//		final String keyword = args[1];

		long start = System.currentTimeMillis();

//		SparkConf conf = new SparkConf().setAppName("JavaParseJson");
//		JavaSparkContext sc = new JavaSparkContext(conf);
//		JavaRDD<String> rawData = sc.textFile(inputpath);
		SparkConf conf = new SparkConf().setMaster("local[2]").setAppName(
				"Elasticsearch");
		conf.set("spark.serializer", KryoSerializer.class.getName());
		conf.set("es.index.auto.create", "true");
		conf.set("es.nodes", "10.26.1.9:9200");
		conf.set("es.resource", "imac/test");
		conf.set("es.input.json" , "true");
		
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		JavaPairRDD<String, Map<String, Object>> esRDD = JavaEsSpark.esRDD(sc, args[0]);
		esRDD.persist(StorageLevel.DISK_ONLY());
		
		final Accumulator<Integer> count = sc.accumulator(0);
		
		
		JavaRDD<String> rawData = esRDD.repartition(1000).map(new Function<Tuple2<String,Map<String,Object>>, String>() {
			public String call(Tuple2<String, Map<String, Object>> arg0) throws Exception {
				return arg0._2.toString();
			}
		});
		
	
		/**
		 * 前處理 將標點符號 符號化
		 */
		JavaRDD<String> tokenizerRDD =rawData.mapPartitions(new FlatMapFunction<Iterator<String>, String>() {
			public Iterable<String> call(Iterator<String> iterator) throws Exception {
				ArrayList<String> arrayList = new ArrayList<String>();
				for(int i=0 ; iterator.hasNext();i++){
					String arg0 = iterator.next();
					if (arg0.contains(":") && !(arg0.split(":")[1].trim().length() == 1)) {
//						uuid++;
						count.add(1);
						String result = count.localValue() + "," + arg0.substring(arg0.indexOf(":") + 1, arg0.length()).replaceAll("\"", "").replaceAll(",", "").trim();
						arrayList.add(result);
					}
				}
				return arrayList;
			}
		}).filter(new Function<String, Boolean>() {
			public Boolean call(String arg0) throws Exception {
				return !arg0.equals("");
			}
		});
		
		
//		JavaRDD<String> tokenizerRDD = rawData.map(new Function<String, String>() {
//			public String call(String arg0) throws Exception {
//				
//				return "";
//			}
//		}).filter(new Function<String, Boolean>() {
//			public Boolean call(String arg0) throws Exception {
//				if (arg0.equals("")) {
//					return false;
//				}
//				return true;
//			}
//		});

		// 將 tokenizerRDD cache 起來，好讓 uuid 值 不再更動
		
		JavaRDD<String> cacheRDD = tokenizerRDD.cache();
		cacheRDD.saveAsTextFile(args[1]);

		JavaPairRDD<String, Integer> tfRDD = tokenizerRDD.flatMapToPair(new PairFlatMapFunction<String, String, Integer>() {
			public Iterable<Tuple2<String, Integer>> call(String arg0) throws Exception {
				ArrayList<Tuple2<String, Integer>> arrayList = new ArrayList<Tuple2<String, Integer>>();
				String[] token = arg0.split(",");
				StringReader reader = new StringReader(token[1]);

				IKSegmenter ik = new IKSegmenter(reader, true);
				Lexeme lexeme = null;

				try {
					while ((lexeme = ik.next()) != null) {
						arrayList.add(new Tuple2<String, Integer>(token[0] + "," + lexeme.getLexemeText(), 1));
					}
				} catch (IOException e) {
					e.printStackTrace();
				} finally {
					reader.close();
				}
				return arrayList;
			}
		}).reduceByKey(new Function2<Integer, Integer, Integer>() {
			public Integer call(Integer arg0, Integer arg1) throws Exception {
				return arg0 + arg1;
			}
		}).sortByKey();
		
		

		JavaRDD<String> tfidfRDD = tfRDD.map(new Function<Tuple2<String, Integer>, String>() {
			public String call(Tuple2<String, Integer> arg0) throws Exception {
				
				String[] token = arg0._1.split(",");
				String term = token[1];
				int termFreq = arg0._2;
				String result = "";
				if (!term.equals(lastTerm)) {
					if (!lastTerm.equals("")) {
						double inverseDocFreq = Math.log10((double) count.localValue() / docCountForTerm);
						double tfidf = termFreq * inverseDocFreq;
						result = lastTerm + "\t" + tfidf;
					}
					lastTerm = term;
					docCountForTerm = 1;
				} else {
					docCountForTerm++;
				}
				return result;
			}
		});

		tfidfRDD.saveAsTextFile(args[1]+"/tfidf_result");
		
		long end = System.currentTimeMillis();
		System.out.println((end-start)/1000+" s");
		
		sc.parallelize(Arrays.asList(((end-start)/1000)+" s")).saveAsTextFile(args[1]+"/result");
	}

}
