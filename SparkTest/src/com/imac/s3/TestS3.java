package com.imac.s3;

import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class TestS3 {

	public static void main(String[] args) {
		JavaSparkContext sc =new JavaSparkContext();
		Configuration conf = sc.hadoopConfiguration();
//		conf.set("fs.s3.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem");
		conf.set("fs.s3n.awsAccessKeyId", "YLQ9SL2N6CLH8UYJ8O61");
		conf.set("fs.s3n.awsSecretAccessKey", "uv3J7VtNsNC5c4378WViXUzD1XHnNIohv5esMRjD");
		ArrayList<Tuple2<String, Integer>> arrayList = new ArrayList<Tuple2<String,Integer>>();
		arrayList.add(new Tuple2<String, Integer>("jack",11));
		arrayList.add(new Tuple2<String, Integer>("jame",12));
		
		JavaPairRDD<String, Integer> rawRDD = sc.parallelizePairs(arrayList);
		rawRDD.saveAsTextFile("s3n://testfile/test.csv");
	}

}
