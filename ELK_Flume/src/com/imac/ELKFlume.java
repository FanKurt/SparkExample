package com.imac;

import java.nio.ByteBuffer;
import java.util.Date;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.serializer.KryoSerializer;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.api.java.JavaStreamingContextFactory;
import org.apache.spark.streaming.flume.FlumeUtils;
import org.apache.spark.streaming.flume.SparkFlumeEvent;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;

public final class ELKFlume {

	public static JavaStreamingContext createContext(String host, int port, String checkpointDirectory) {

		Duration batchInterval = new Duration(1000);
		SparkConf sparkConf = new SparkConf().setAppName("JavaELKFlume");
		sparkConf.set("spark.serializer", KryoSerializer.class.getName());
		sparkConf.set("es.index.auto.create", "true");
		sparkConf.set("es.nodes", "10.26.1.9:9200");
		sparkConf.set("es.resource", "opestack_error/list");
		sparkConf.set("es.input.json", "true");

		JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, batchInterval);
		ssc.checkpoint(checkpointDirectory);

		JavaReceiverInputDStream<SparkFlumeEvent> flumeStream = FlumeUtils.createStream(ssc, host, port);

		JavaDStream<String> flume_data = flumeStream.map(new Function<SparkFlumeEvent, String>() {
			public String call(SparkFlumeEvent arg0) throws Exception {
				ByteBuffer bytePayload = arg0.event().getBody();
				return new String(bytePayload.array());
			}
		});

		flume_data.print();

		flume_data.foreachRDD(new Function<JavaRDD<String>, Void>()  {
			public Void call(JavaRDD<String> arg0) throws Exception {
				if (!arg0.isEmpty()) {
					JavaRDD<String> rdd = arg0.filter(new Function<String, Boolean>() {
						public Boolean call(String arg0) throws Exception {
							return arg0.contains("ERROR") || arg0.contains("Exception");
						}
					});
					
					JavaEsSpark.saveToEs(rdd, "opestack_error/list");
				}
				return null;
			}
		});

		return ssc;
	}

	public static void main(String[] args) {
		if (args.length != 3) {
			System.err.println("Usage: JavaFlumeEventCount <host> <port> <checkpoint>");
			System.exit(1);
		}

		final String host = args[0];
		final int port = Integer.parseInt(args[1]);
		final String checkpointDirectory = args[2];

		JavaStreamingContextFactory factory = new JavaStreamingContextFactory() {
			public JavaStreamingContext create() {
				return createContext(host, port, checkpointDirectory);
			}
		};

		JavaStreamingContext jssc = JavaStreamingContext.getOrCreate(checkpointDirectory, factory);

		jssc.start();
		jssc.awaitTermination();
	}

	private static String getNowTime() {
		Date now = new Date();
		String[] token = now.toString().split(" ");
		String time = token[token.length - 1] + "-" + tranferMonth(token[1]) + "-" + token[2] + "T" + token[3] + "Z";
		return time;
	}

	private static String tranferMonth(String month) {
		String[] arrStrings = { "Jan", "Feb", "Mar", "Apr", "Mar", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec" };
		for (int i = 0; i < arrStrings.length; i++) {
			if (month.equals(arrStrings[i])) {
				return (i < 9) ? "0" + (i + 1) : "" + (i + 1);
			}
		}
		return "";
	}
}