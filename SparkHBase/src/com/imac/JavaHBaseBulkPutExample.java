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
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import com.cloudera.spark.hbase.JavaHBaseContext;
import com.imac.JavaHBaseBulkGetExample.GetFunction;
import com.imac.JavaHBaseBulkGetExample.ResultFunction;

public class JavaHBaseBulkPutExample {
	private static int i = 0 ;
  public static void main(String args[]) {
    if (args.length == 0) {
      System.out
          .println("JavaHBaseBulkPutExample  {master} {tableName} {columnFamily}");
    }

    String master = args[0];
    String tableName = args[1];
    final String columnFamily = args[2];
    String input = args[3];

    JavaSparkContext jsc = new JavaSparkContext(master,
        "JavaHBaseBulkPutExample");

//    List<String> list = new ArrayList<String>();
//    list.add("1," + columnFamily + ",a,1");
//    list.add("2," + columnFamily + ",a,2");
//    list.add("3," + columnFamily + ",a,3");
//    list.add("4," + columnFamily + ",a,4");
//    list.add("5," + columnFamily + ",a,5");
//    JavaRDD<String> rdd = jsc.parallelize(list);
    JavaRDD<String> inputData = jsc.textFile(input);
    JavaRDD<String> rdd  =inputData.flatMap(new FlatMapFunction<String, String>() {
		public Iterable<String> call(String arg0) throws Exception {
			return Arrays.asList(arg0.split("\\|"));
		}
	}).map(new Function<String, String>() {
		public String call(String arg0) throws Exception {
			return "1"+","+columnFamily+","+arg0+","+1;
		}
	});
    
    Configuration conf = HBaseConfiguration.create();
    conf.addResource(new Path("/opt/hadoop-2.6.0/etc/hadoop/core-site.xml"));
    conf.addResource(new Path("/opt/hbase/conf/hbase-site.xml"));

    JavaHBaseContext hbaseContext = new JavaHBaseContext(jsc, conf);
    long start = System.currentTimeMillis();
    hbaseContext.bulkPut(rdd, tableName, new PutFunction(), true);
	long end = System.currentTimeMillis();
	System.out.println("FINAL :	 "+((end-start)/1000) + "  s");
	
//	jsc.parallelize(Arrays.asList(((end-start)/1000)+"")).saveAsTextFile("/hbase_result");
	
	
    jsc.stop();
    
  }

  public static class PutFunction implements Function<String, Put> {

    private static final long serialVersionUID = 1L;

    public Put call(String v) throws Exception {
      String[] cells = v.split(",");
      Put put = new Put(Bytes.toBytes(cells[0]));

      put.add(Bytes.toBytes(cells[1]), Bytes.toBytes(cells[2]),
          Bytes.toBytes(cells[3]));
      return put;
    }

  }

}