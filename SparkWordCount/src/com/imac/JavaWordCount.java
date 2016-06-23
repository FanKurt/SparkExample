package com.imac;

import scala.Tuple2;

import org.apache.spark.Partitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;

public final class JavaWordCount {
  private static final Pattern SPACE = Pattern.compile(" ");

  public static void main(String[] args) throws Exception {

//    if (args.length < 2) {
//      System.err.println("Usage: JavaWordCount <file>");
//      System.exit(1);
//    }
    long star_time = System.currentTimeMillis();
    
    SparkConf conf = new SparkConf().setAppName("JavaWordCount");
//    conf.set("spark.mesos.coarse", args[2]);
    JavaSparkContext ctx = new JavaSparkContext(conf);
   
    JavaRDD<String> lines= ctx.textFile(args[0],1);
    
    lines.foreach(new VoidFunction<String>() {
		public void call(String arg0) throws Exception {
			System.out.println(arg0);
		}
	});
    
    
    
    // 10G
//    JavaRDD<String> line = lines.union(lines);
////    // 40 G
//    JavaRDD<String> line1 = line.union(line).union(line).union(line);
////    // 80 G
//    JavaRDD<String> line2 = line1.union(line1);
//    // 120 G
//    JavaRDD<String> line3 = lines.union(lines).union(lines);
//    // 200 G
//    JavaRDD<String> line4 = line1.union(line1).union(line1).union(line1).union(line1);

//    JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
//      public Iterable<String> call(String s) {
//        return Arrays.asList(s.split("|"));
//      }
//    });
//
//    JavaPairRDD<String, Integer> ones = words.mapToPair(new PairFunction<String, String, Integer>() {
//      public Tuple2<String, Integer> call(String s) {
//        return new Tuple2<String, Integer>(s, 1);
//      }
//    });
//
//    JavaPairRDD<String, Integer> counts = ones.reduceByKey(new Function2<Integer, Integer, Integer>() {
//      public Integer call(Integer i1, Integer i2) {
//        return i1 + i2;
//      }
//    });
//
//    List<Tuple2<String, Integer>> output = counts.collect();
//    for (Tuple2<?,?> tuple : output) {
//      System.out.println(tuple._1() + ": " + tuple._2());
//    }
////    lines.repartition(1).saveAsTextFile(args[1]);
//    
//    long end_time = System.currentTimeMillis();
//    System.out.println("=======================");
//    System.out.println("Time : "+(end_time-star_time)/1000+" s");
//    System.out.println("=======================");
    ctx.stop();
  }
}