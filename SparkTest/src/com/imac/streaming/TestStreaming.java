package com.imac.streaming;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import scala.Tuple2;

public class TestStreaming {

	public static void main(String[] args) {
//		if(args.length!=2){
//			System.out.println("Format Error: {hostname} {port}");
//		}
		SparkConf conf = new SparkConf();
		conf.setAppName("TestStreaming");
		JavaStreamingContext sc = new JavaStreamingContext(conf, Durations.milliseconds(Integer.parseInt(args[2])));
		String hostname = args[0];
		int port = Integer.parseInt(args[1]);
		JavaReceiverInputDStream<String> input = sc.socketTextStream(hostname, port);
		input.flatMap(new FlatMapFunction<String, String>() {
			public Iterable<String> call(String arg0) throws Exception {
				return Arrays.asList(arg0.split(" "));
			}
		}).mapToPair(new PairFunction<String, String, Integer>() {
			public Tuple2<String, Integer> call(String arg0) throws Exception {
				return new Tuple2<String, Integer>(arg0, 1);
			}
		}).reduceByKey(new Function2<Integer, Integer, Integer>() {
			public Integer call(Integer arg0, Integer arg1) throws Exception {
				return arg0+arg1;
			}
		}).print();
		
		
		sc.start();
		sc.awaitTermination();
		
	}

}
