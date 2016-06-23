package com.imac;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.StorageLevels;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.storage.StorageLevel;
import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

import scala.Tuple2;

public class JavaParseJson  {
	private static int uuid = 0;
	private static String lastTerm= "";
	private static int numberOfDocuments, docCountForTerm;
	public static HashMap<String, Double> compositeKeyHashMap = new HashMap<String, Double>(); // 給 CompositeKey 進行記錄用的，不然其size永遠是0
	public static void main(String[] args) throws Exception {
		
		String inputpath = args[0];
		final String keyword = args[1];
		
		long start = System.currentTimeMillis();
		
		SparkConf conf = new SparkConf().setAppName("JavaParseJson");
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<String> rawData = sc.textFile(inputpath);
		
		
		/**
		 *  前處理  將標點符號 符號化
		 */
		
		JavaRDD<String> tokenizerRDD =rawData.map(new Function<String, String>() {
			public String call(String arg0) throws Exception {
				uuid++;
				if(arg0.contains(":") && !(arg0.split(":")[1].trim().length() == 1)){
					return uuid+","+arg0.substring(arg0.indexOf(":")+1,arg0.length()).replaceAll("\"", "").replaceAll(",", "").trim();
				}
				return "";
			}
		}).filter(new Function<String, Boolean>() {
			public Boolean call(String arg0) throws Exception {
				if(arg0.equals("")){
					return false;
				}
				return true;
			}
		});
		
		//將 tokenizerRDD cache 起來，好讓 uuid 值 不再更動
		JavaRDD<String> cacheRDD = tokenizerRDD.cache();
		
		long total_documents = cacheRDD.count();

		System.out.println("-------------------");
		System.out.println("documentOfCount :"+total_documents);
		System.out.println("-------------------");
	
		
		numberOfDocuments =(int) total_documents;
		
//		cacheRDD.foreach(new VoidFunction<String>() {
//			public void call(String arg0) throws Exception {
//				System.out.println(arg0);
//			}
//		});
	
		/**
		 * 二次排序
		 */
		
		//給定分詞資料以及相關資料準備做二次排序
		JavaPairRDD<String, Iterable<String>> prepareRDD = cacheRDD.flatMapToPair(new PairFlatMapFunction<String, String, String>() {
			public Iterable<Tuple2<String, String>> call(String arg0)
					throws Exception {
				String [] token = arg0.split(",");
				StringReader reader = new StringReader(token[1]);

				IKSegmenter ik = new IKSegmenter(reader, true);
				Lexeme lexeme = null;
				
				ArrayList<Tuple2<String, String>> tupleList = new ArrayList<>();
				try {
					while ((lexeme = ik.next()) != null) {
//						System.out.println(lexeme.getLexemeText());
						tupleList.add(new Tuple2<>(lexeme.getLexemeText(),token[0]+","+true+","+1.0));
						tupleList.add(new Tuple2<>(lexeme.getLexemeText(),token[0]+","+false+","+1.0));
					}
				} catch (IOException e) {
					e.printStackTrace();
				} finally {
					reader.close();
				}
				return tupleList;
			} 
		}).groupByKey().sortByKey();
		
		
		
//		prepareRDD.foreach(new VoidFunction<Tuple2<String,Iterable<String>>>() {
//			public void call(Tuple2<String, Iterable<String>> arg0) throws Exception {
//				System.out.println(arg0);
//			}
//		});

		
//		for(Tuple2<String, Iterable<String>> v : prepareRDD.collect()){
//			System.out.println(v._1+","+v._2);
//		}
		
		//針對 groupByKey處理完的結果進行2次排序 ， true > false
		/**
		 * example
		 * (a,1,true) 1 
		 * (a,3,true) 1 
		 * (a,1,false) 1
		 * (a,4,false) 1 
		 * ....
		 */
		JavaPairRDD<String, Double> secondSort = prepareRDD.flatMapToPair(new PairFlatMapFunction<Tuple2<String,Iterable<String>>,String, Double>() {
			public Iterable<Tuple2<String, Double>> call(
					Tuple2<String, Iterable<String>> arg0) throws Exception {
				
				ArrayList<CompositeKey> mlKeys = new ArrayList<CompositeKey>();
				Iterator<String> iterator = arg0._2.iterator();
				
				double count = 1.0;
				while(iterator.hasNext()){
					String value = iterator.next();
					String [] token = value.split(",");
					
					mlKeys.add(new CompositeKey(token[0],token[1],count));
				}
				
				Collections.sort(mlKeys);
				
				ArrayList<Tuple2<String, Double>> resulArrayList = new ArrayList<Tuple2<String, Double>>();
				for(CompositeKey cKey : mlKeys){
					resulArrayList.add(new Tuple2<String, Double>(arg0._1+","+cKey.getID()+","+cKey.getBoo(), cKey.getDouble(cKey.getID()+cKey.getBoo())));
				}
				
				return removeDuplicateWithOrder(resulArrayList);
			}
		});
		
//		secondSort.saveAsTextFile(args[2]);
//		
//		for(String v : sc.textFile(args[2]).collect()){
//			System.out.println(v);
//		}
		
		
//		secondSort.foreach(new VoidFunction<Tuple2<String,Double>>() {
//			public void call(Tuple2<String, Double> arg0) throws Exception {
//				System.out.println(arg0);
//			}
//		});
	
		//統計數量
		JavaPairRDD<String, Double> broacastRDD = secondSort.reduceByKey(new Function2<Double, Double, Double>() {
			public Double call(Double arg0, Double arg1) throws Exception {
				return arg0 + arg1;
			}
		});
		
		
//		//將結果存入廣播變數
		final Broadcast<Map<String, Double>> broacastMap = sc.broadcast(broacastRDD.collectAsMap());
	
//		//將重覆資料去除
		JavaPairRDD<String, Double> reduceRDD = prepareRDD.flatMapToPair(new PairFlatMapFunction<Tuple2<String,Iterable<String>>,String, Double>() {
			public Iterable<Tuple2<String, Double>> call(
					Tuple2<String, Iterable<String>> arg0) throws Exception {
				
				ArrayList<CompositeKey> mlKeys = new ArrayList<CompositeKey>();
				Iterator<String> iterator =arg0._2.iterator();
				double count = 1.0;
				while(iterator.hasNext()){
					String value = iterator.next();
					String [] token = value.split(",");
					
					mlKeys.add(new CompositeKey(token[0],token[1],count));
				}
				
				Collections.sort(mlKeys);
				
				ArrayList<Tuple2<String, Double>> resulArrayList = new ArrayList<Tuple2<String, Double>>();
				for(CompositeKey cKey : mlKeys){
					resulArrayList.add(new Tuple2<String, Double>(arg0._1+","+cKey.getID()+","+cKey.getBoo(), cKey.getDouble(cKey.getID()+cKey.getBoo())));
				}
				
				return removeDuplicateWithOrder(resulArrayList);
			}
		})
		//透過廣播變數將正確數量做對應 (原因是為了滿足 2次排序 且 也可統計數量)
		.mapToPair(new PairFunction<Tuple2<String,Double>, String, Double>() {
			public Tuple2<String, Double> call(Tuple2<String, Double> arg0)
					throws Exception {
				return new Tuple2<String, Double>(arg0._1, broacastMap.value().get(arg0._1)) ;
			}
		});
		
		
		//TF-IDF演算法
		JavaPairRDD<String, HashMap<String, Double>> tfidfRDD =reduceRDD.mapToPair(new PairFunction<Tuple2<String,Double>, String, HashMap<String, Double>>() {
			public Tuple2<String, HashMap<String, Double>> call(
					Tuple2<String, Double> arg0) throws Exception {
				String [] token = arg0._1.split(",");
				String term =  token[0];
				String docID = token[1];
				boolean dfEntry = Boolean.parseBoolean(token[2]);
				Double value = arg0._2;
				String outTuple ="";
				HashMap<String, Double> map = new HashMap<>();
				if (!term.equals(lastTerm)) {
					docCountForTerm = value.intValue();
					lastTerm = term;
				} else {
					if (dfEntry) {
						docCountForTerm+=value;
					} else {
						double termFreq = 0;
						termFreq = arg0._2;
						double inverseDocFreq = Math.log10((double) numberOfDocuments
								/ docCountForTerm);
						double tfidf = termFreq * inverseDocFreq;
						/* Verbose output for learning purposes. */
						outTuple = "(" + term + ", " + docID + ")" + " [tf:"
								+ termFreq + " n:" + docCountForTerm + " N:"
								+ numberOfDocuments + "]";
						
						map.put(term, tfidf);
					}
				}
			
				return new Tuple2<String, HashMap<String,Double>>(term + ", " + docID, map);
			}
		}).filter(new Function<Tuple2<String,HashMap<String,Double>>, Boolean>() {
			public Boolean call(Tuple2<String, HashMap<String, Double>> arg0)
					throws Exception {
				return (arg0._2.size()>0);
			}
		});
		
		
		//將搜尋文字與document進行 Cosine相似度
//		final Map<String, HashMap<String, Double>> tfidfMap =tfidfRDD.collectAsMap();
//		final Broadcast<Map<String, HashMap<String, Double>>> tfidfBroacast =sc.broadcast(tfidfMap);
//		
//		HashMap<String, Double> finalSimilar = new HashMap<>();
//		
//		Set<String> keyString =tfidfMap.keySet();
//	
//		for(String key : keyString){
//			Double similar = CosineSimilarity.calculateCosineSimilarity(
//					tfidfBroacast.value().get(key),
//					getQueryWord(keyword));
//			finalSimilar.put(key, similar);
//		}
//
//		Map<String, Double> hasMap = sortByValue(finalSimilar);
//		
//
//		Iterator<String> iterator = hasMap.keySet().iterator();
//		final ArrayList<String> result = new ArrayList<String>();
//		
//		for (int i = 0; iterator.hasNext() ; i++) {
//			String key = iterator.next();
//			if(hasMap.get(key) != 0.0){
//				System.out.println("-------------------");
//				System.out.println((i+1)+","+key+"	"+hasMap.get(key));
//				System.out.println("-------------------");
//				result.add((i+1)+","+key);
//			}
//		}
//		if(result.size()==0){
//			result.add("No search similar document....");
//		}
//		
//		
//		final Broadcast<ArrayList<String>> originBroacast = sc.broadcast(result);
//		
//		
//		JavaPairRDD<Integer, String> resultRDD = cacheRDD.mapToPair(new PairFunction<String, Integer , String>() {
//			public Tuple2<Integer, String> call(String arg0) throws Exception {
//				ArrayList<String> originList = originBroacast.value();
//					String [] data = arg0.split(",");
//					for(String value : originList){
//						if(!value.equals("No search similar document....")){
//							String [] token = value.trim().replaceAll(" ", "").split(",");
//							if (data[1].toLowerCase().contains(token[1])&& data[0].toLowerCase().equals(token[2])) {
//								return new Tuple2<Integer, String>(Integer.parseInt(token[0]),arg0);
//							}
//						}
//					}
//				return new Tuple2<Integer, String>(12345, "");
//			}
//		}).filter(new Function<Tuple2<Integer,String>, Boolean>() {
//			public Boolean call(Tuple2<Integer, String> arg0) throws Exception {
//				if(arg0._1 != 12345 && arg0._2.length()>0 ){
//					return true;
//				}
//				return false;
//			}
//		}).sortByKey();
//		
//		
//		List<Tuple2<Integer, String>> resultList = resultRDD.collect();
//		if(resultList.size()>0){
//			for(Tuple2<Integer, String> arg0 : resultList){
//				String [] token = arg0._2.split(",");
//				System.out.println("-------------------");
//				System.out.println(arg0._1+".	document ID : "+token[0]+"\r\n	Text :  "+token[1].trim());
//				System.out.println("-------------------");
//			}
//		}else{
//			System.out.println("-------------------");
//			System.out.println("No search similar document....");
//			System.out.println("-------------------");
//		}
		
		tfidfRDD.saveAsTextFile(args[2]);
		long end = System.currentTimeMillis();
		System.out.println("Total	"+(end-start)/1000+"	s");
		sc.stop();
	}
	
	/**
	 * 去除 ArrayList 重覆的部分
	 * @param resulArrayList
	 * @return ArrayList
	 */
//	public static ArrayList<Tuple2<String, Double>> removeDuplicateWithOrder(ArrayList<Tuple2<String, Double>> arlList) {
//		Set set = new HashSet();
//		List<Tuple2<String, Double>> newList = new ArrayList<Tuple2<String, Double>>();
//		for (Iterator iter = arlList.iterator(); iter.hasNext();) {
//			Tuple2<String, Double> element = (Tuple2<String, Double>) iter.next();
//			if (set.add(element))
//				newList.add(element);
//		}
//		arlList.clear();
//		arlList.addAll(newList);
//		return arlList;
//	}
	public static ArrayList<Tuple2<String, Double>> removeDuplicateWithOrder(ArrayList<Tuple2<String, Double>> resulArrayList) {
		Set set = new HashSet();
		List<Tuple2<String, Double>> newList = new ArrayList<Tuple2<String, Double>>();
		Iterator iter = resulArrayList.iterator();
		
		while(iter.hasNext()){
			Tuple2<String, Double> element = (Tuple2<String, Double>) iter.next();
			if (set.add(element)){
				newList.add(element);
			}
		}
		
		resulArrayList.clear();
		resulArrayList.addAll(newList);
		return resulArrayList;
	}
	
	/**
	 * 取得 關鍵字搜尋的 HashMap
	 * @param conf
	 * @return HashMap
	 */
	private static HashMap<String, Double> getQueryWord(String keyword) {
		HashMap<String, Double> queryWord = new HashMap<>();
		StringReader reader = new StringReader(keyword);
		IKSegmenter ik = new IKSegmenter(reader, true);
		Lexeme lexeme = null;
		try {
			while ((lexeme = ik.next()) != null) {
				queryWord.put(lexeme.getLexemeText(), (double) 1);
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			reader.close();
		}
		return queryWord;
	}
	
	/**
	 * 排序
	 * @param map 尚未排序的HashMap
	 * @return 排序完成的HashMap
	 */
	public static <K, V extends Comparable<? super V>> Map<K, V> sortByValue(
			Map<K, V> map) {
		LinkedList<java.util.Map.Entry<K, V>> list = new LinkedList<>(
				map.entrySet());
		Collections.sort(list, new Comparator<Map.Entry<K, V>>() {
			@Override
			public int compare(Map.Entry<K, V> o1, Map.Entry<K, V> o2) {
				return (o2.getValue()).compareTo(o1.getValue());
			}
		});
		Map<K, V> result = new LinkedHashMap<>();
		for (Map.Entry<K, V> entry : list) {
			result.put(entry.getKey(), entry.getValue());
		}
		return result;
	}
}