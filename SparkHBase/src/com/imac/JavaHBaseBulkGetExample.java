package com.imac;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import com.cloudera.spark.hbase.JavaHBaseContext;

public class JavaHBaseBulkGetExample {
	public static void main(String args[]) {
		if (args.length == 0) {
			System.out.println("JavaHBaseBulkGetExample  {master} {tableName}");
		}

		String master = args[0];
		String tableName = args[1];
		JavaSparkContext jsc = new JavaSparkContext(master,
				"JavaHBaseBulkGetExample");

		List<byte[]> list = new ArrayList<byte[]>();
		list.add(Bytes.toBytes("1"));
//		list.add(Bytes.toBytes("2"));
//		list.add(Bytes.toBytes("3"));
//		list.add(Bytes.toBytes("4"));
//		list.add(Bytes.toBytes("5"));

		JavaRDD<byte[]> rdd = jsc.parallelize(list);

		Configuration conf = HBaseConfiguration.create();
		conf.addResource(new Path("/opt/hadoop-2.6.0/etc/hadoop/core-site.xml"));
		conf.addResource(new Path("/opt/hbase/conf/hbase-site.xml"));

		long start = System.currentTimeMillis();
		JavaHBaseContext hbaseContext = new JavaHBaseContext(jsc, conf);
		JavaRDD<String> data = hbaseContext.bulkGet(tableName, 2, rdd,
				new GetFunction(), new ResultFunction());

//		data.collect();
	
//		jsc.parallelize(Arrays.asList(((end - start) / 1000) + "")).saveAsTextFile("/hbase_result");
	
	
		 for (String v : data.collect()) {
			 System.out.println(v);
		 }
		long end = System.currentTimeMillis();
		System.out.println("GET FINAL :	 " + ((end - start) / 1000) + "  s");
		jsc.stop();
	}

	public static class GetFunction implements Function<byte[], Get> {

		private static final long serialVersionUID = 1L;

		public Get call(byte[] v) throws Exception {
			return new Get(v);
		}
	}

	public static class ResultFunction implements Function<Result, String> {

		private static final long serialVersionUID = 1L;

		public String call(Result result) throws Exception {
			Iterator<KeyValue> it = result.list().iterator();
			StringBuilder b = new StringBuilder();

			b.append(Bytes.toString(result.getRow()) + ":");

			while (it.hasNext()) {
				KeyValue kv = it.next();
				String q = Bytes.toString(kv.getQualifier());
				if (q.equals("counter")) {
					b.append("(" + Bytes.toString(kv.getQualifier()) + ","
							+ Bytes.toLong(kv.getValue()) + ")");
				} else {
					b.append("(" + Bytes.toString(kv.getQualifier()) + ","
							+ Bytes.toString(kv.getValue()) + ")");
				}
			}
			return b.toString();
		}
	}
}