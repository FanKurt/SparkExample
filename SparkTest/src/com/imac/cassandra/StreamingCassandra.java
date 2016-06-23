package com.imac.cassandra;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.javaFunctions;
import static com.datastax.spark.connector.japi.CassandraJavaUtil.mapToRow;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;

import kafka.serializer.StringDecoder;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.examples.streaming.StreamingExamples;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import scala.Tuple2;

public class StreamingCassandra {
	private static int count = 0;

	public static void main(String[] args) {
		if (args.length < 2) {
			System.err.println("Usage: JavaDirectKafkaWordCount <brokers> <topics>\n" + "  <brokers> is a list of one or more Kafka brokers\n"
					+ "  <topics> is a list of one or more kafka topics to consume from\n\n");
			System.exit(1);
		}

		StreamingExamples.setStreamingLogLevels();

		String brokers = args[0];
		String topics = args[1];
		int sec = Integer.parseInt(args[2]);
		// Create context with a 2 seconds batch interval
		SparkConf sparkConf = new SparkConf().setAppName("JavaDirectKafkaWordCount");
		JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(sec));

		HashSet<String> topicsSet = new HashSet<String>(Arrays.asList(topics.split(",")));
		HashMap<String, String> kafkaParams = new HashMap<String, String>();
		kafkaParams.put("metadata.broker.list", brokers);

		// Create direct kafka stream with brokers and topics
		JavaPairInputDStream<String, String> messages = KafkaUtils.createDirectStream(jssc, String.class, String.class, StringDecoder.class, StringDecoder.class, kafkaParams,
				topicsSet);
		
		JavaDStream<String> lines = messages.map(new Function<Tuple2<String, String>, String>() {
			public String call(Tuple2<String, String> tuple2) {
				return tuple2._2();
			}
		});

		JavaDStream<EMP> cassandraRDD = lines.map(new Function<String, EMP>() {
			public EMP call(String arg0) throws Exception {
				String[] token = arg0.split(" ");
				return EMP.newInstance(count++, token[3], token[token.length - 1], null, null);
			}
		});
		cassandraRDD.foreachRDD(new Function<JavaRDD<EMP>, Void>() {
			public Void call(JavaRDD<EMP> rdd) throws Exception {
				javaFunctions(rdd).writerBuilder("spark_smack", "emp", mapToRow(EMP.class)).saveToCassandra();
				return null;
			}
		});


		jssc.start();
		jssc.awaitTermination();

	}

}
