package com.imac;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.catalyst.expressions.In;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import scala.Tuple2;

public class EtuTest {

	public static void main(String[] args) {
		SparkConf sparkConf = new SparkConf().setAppName("ETUSparkSQL");
	    JavaSparkContext sc = new JavaSparkContext(sparkConf);
	    SQLContext sqlContext = new SQLContext(sc);
	    
	    String sourcePath1 = args[0];
		String sourcePath2 = args[1];

		JavaRDD<Row> viewRDD = sc.textFile(sourcePath1).map(new Function<String, Row>() {
			public Row call(String arg0) throws Exception {
				String [] split = arg0.split(",");
				if(!arg0.contains("dates")){
					return RowFactory.create(split[0] , split[1] , split[2] , split[3] , split[4] , split[5]);
				}
				return null;
			}
		}).filter(new Function<Row, Boolean>() {
			public Boolean call(Row arg0) throws Exception {
				return arg0!=null;
			}
		});
		
		JavaRDD<Row> orderRDD = sc.textFile(sourcePath2).map(new Function<String, Row>() {
			public Row call(String arg0) throws Exception {
				String [] split = arg0.split(",");
				if(!arg0.contains("dates")){
					return RowFactory.create(split[0] , split[1] , split[2] , split[3] , split[4] , split[5], split[6]);
				}
				return null;
			}
		}).filter(new Function<Row, Boolean>() {
			public Boolean call(Row arg0) throws Exception {
				return arg0!=null;
			}
		});
		
		
	    DataFrame viewDataFrame = sqlContext.createDataFrame(viewRDD, createViewStructType());
	    viewDataFrame.registerTempTable("viewData");
	    
//	    DataFrame june = viewDataFrame.sqlContext().sql("select dates from viewData where dates >= '2015-06-01' and dates <= '2015-06-31'");
//	    june.toJavaRDD().mapToPair(new PairFunction<Row, String, String>() {
//			public Tuple2<String, String> call(Row arg0) throws Exception {
//				return new Tuple2<String, String>(arg0.get(0).toString() , arg0.get(1).toString());
//			}
//		});
//	    june.printSchema();
//	    for(Row value : june.take(30)){
//	    	System.out.println(value.toString());
//	    }
	    
	    DataFrame orderDataFrame = sqlContext.createDataFrame(orderRDD, createOrderStructType());
	    orderDataFrame.registerTempTable("orderData");
	    
	    
	   JavaPairRDD<String, String> orderJuneDataFrame = getOrderJuneRDD(orderDataFrame);
	    
	   JavaPairRDD<String, String> orderJulyDataFrame = getOrderJulyRDD(orderDataFrame);
	   
	   JavaPairRDD<String, String> orderAugustDataFrame = getOrderAugustRDD(orderDataFrame);
	   
	   
	   JavaPairRDD<String, Tuple2<Tuple2<String, String>, String>> oderJoinRDD = orderJuneDataFrame.join(orderJulyDataFrame).join(orderAugustDataFrame);
	   
	   JavaPairRDD<String, String> orderAverageRDD = oderJoinRDD.mapToPair(new PairFunction<Tuple2<String,Tuple2<Tuple2<String,String>,String>>, String, String>() {
			public Tuple2<String, String> call(
					Tuple2<String, Tuple2<Tuple2<String, String>, String>> arg0)
					throws Exception {
				String userid = arg0._1();
				String[] june = arg0._2()._1()._1().split("_");
				String[] july = arg0._2()._1()._2().split("_");
				String[] august = arg0._2()._2().split("_");
				int average_consump_count = (Integer.parseInt(june[1])+Integer.parseInt(july[1])+Integer.parseInt(august[1]))/3;
				int average_consump_money = (Integer.parseInt(june[2])+Integer.parseInt(july[2])+Integer.parseInt(august[2]))/3;
				String output = (june[0]+","+july[0]+","+august[0])+"_"+average_consump_count+"_"+average_consump_money;
				return new Tuple2<String, String>(userid, output);
			}
	   });
	   
	  JavaPairRDD<String, String> predictRDD =  orderAverageRDD.mapToPair(new PairFunction<Tuple2<String,String>, String , String>() {
		public Tuple2<String, String> call(Tuple2<String, String> arg0)
				throws Exception {
			String [] split = arg0._2().split("_");
			String [] shops = split[0].split(",");
			List<Integer> shopArray = Arrays.asList(Integer.parseInt(shops[0]),Integer.parseInt(shops[1]),Integer.parseInt(shops[2]));
			int predictShop = maxRepeating(shopArray.toArray());
			return new Tuple2<String, String>(arg0._1(), predictShop+"_"+split[1]+"_"+split[2]);
		}
	});
	   
	   
//	   System.out.println("6-8 Avg : "+orderAverageRDD.count());
	   
	   final Map<String, String>  orderAverageMap = predictRDD.collectAsMap();
	   
	   
	   JavaPairRDD<String, String> september =getOrderSeptembertRDD(orderDataFrame);
	   
	   JavaPairRDD<String, String> evaluateRDD = september.filter(new Function<Tuple2<String,String>, Boolean>() {
			public Boolean call(Tuple2<String, String> arg0) throws Exception {
				return !orderAverageMap.containsKey(arg0._1());
			}
	   }).mapToPair(new PairFunction<Tuple2<String,String>, String, String>() {
			public Tuple2<String, String> call(Tuple2<String, String> arg0)
					throws Exception {
//				return new Tuple2<String, String>(arg0._1()+" : "+arg0._2(),orderAverageMap.get(arg0._1()));
				return new Tuple2<String, String>(arg0._1(),arg0._2());
			}
	   });
	   
//	  double accuracy =  evaluateRDD.filter(new Function<Tuple2<String,String>, Boolean>() {
//			public Boolean call(Tuple2<String, String> arg0) throws Exception {
//				String label = arg0._1().split("_")[0];
//				String predict = arg0._2().split("_")[0];
//				return label.equals(predict);
//			}
//		}).count() / (double)predictRDD.count();
	   
	   
//	   JavaPairRDD<String, String> evaluateRDD =  september.map(new Function<Row, String>() {
//			public String call(Row arg0) throws Exception {
//				return arg0.get(6).toString()+"_"+arg0.get(0)+"_"+arg0.get(4)+"_"+arg0.get(5);
//			}
//		}).filter(new Function<String, Boolean>() {
//			public Boolean call(String arg0) throws Exception {
//				return orderAverageMap.containsKey(arg0.split("_")[0]);
//			}
//		}).mapToPair(new PairFunction<String, String, String>() {
//			public Tuple2<String, String> call(String arg0) throws Exception {
//				return new Tuple2<String, String>(arg0,orderAverageMap.get(arg0.split("_")[0]));
//			}
//		});
	    
	    
	    for(Tuple2<String, String> value : evaluateRDD.collect()){
	    	System.out.println(value.toString());
	    }
	    
//	    System.out.println("accuracy : "+accuracy);
	    
	    sc.stop();
	}
	
	private static StructType createViewStructType() {
		List<StructField> fields = new ArrayList<>();
		fields.add(DataTypes.createStructField("dates", DataTypes.StringType, true));
	    fields.add(DataTypes.createStructField("time", DataTypes.StringType, true));
	    fields.add(DataTypes.createStructField("storeid", DataTypes.StringType, true));
	    fields.add(DataTypes.createStructField("catid_1", DataTypes.StringType, true));
	    fields.add(DataTypes.createStructField("catid_2", DataTypes.StringType, true));
	    fields.add(DataTypes.createStructField("userid", DataTypes.StringType, true));
	    StructType schema = DataTypes.createStructType(fields);
		return schema;
	}
	
	private static StructType createOrderStructType() {
		List<StructField> fields = new ArrayList<>();
	    fields.add(DataTypes.createStructField("storeid", DataTypes.StringType, true));
	    fields.add(DataTypes.createStructField("catid_1", DataTypes.StringType, true));
	    fields.add(DataTypes.createStructField("catid_2", DataTypes.StringType, true));
	    fields.add(DataTypes.createStructField("orderno", DataTypes.StringType, true));
	    fields.add(DataTypes.createStructField("datestime", DataTypes.StringType, true));
	    fields.add(DataTypes.createStructField("amt", DataTypes.StringType, true));
	    fields.add(DataTypes.createStructField("userid", DataTypes.StringType, true));
	    StructType schema = DataTypes.createStructType(fields);
		return schema;
	}
	
	private static JavaPairRDD<String, String> getOrderJuneRDD (DataFrame orderDataFrame){
		DataFrame june = orderDataFrame.sqlContext().sql("select * from orderData where datestime >= '20150601000000' and datestime <= '20150630000000'");
	    JavaPairRDD<String, String> userConsumeShop = june.toJavaRDD().mapToPair(new PairFunction<Row, String, String>() {
			public Tuple2<String, String> call(Row arg0) throws Exception {
				return new Tuple2<String, String>(arg0.get(6).toString() , arg0.get(0)+"_"+arg0.get(4)+"_"+arg0.get(5));
			}
		}).groupByKey().mapToPair(new PairFunction<Tuple2<String,Iterable<String>>, String, String>() {
			public Tuple2<String, String> call(
					Tuple2<String, Iterable<String>> arg0) throws Exception {
				int total =0;
				int count=0;
				String shop ="";
				for(String value : arg0._2()){
					String [] split = value.split("_");
					total+= Integer.parseInt(split[split.length-1]);
					shop = split[0];
					count++;
				}
				
				String output = shop+"_"+count+"_"+(total/count);
				return new Tuple2<String, String>(arg0._1(),output);
			}
		});
		return userConsumeShop;
	}
	
	private static JavaPairRDD<String, String> getOrderJulyRDD (DataFrame orderDataFrame){
		DataFrame july = orderDataFrame.sqlContext().sql("select * from orderData where datestime >= '20150701000000' and datestime <= '20150731000000'");
	    JavaPairRDD<String, String> userConsumeShop = july.toJavaRDD().mapToPair(new PairFunction<Row, String, String>() {
			public Tuple2<String, String> call(Row arg0) throws Exception {
				return new Tuple2<String, String>(arg0.get(6).toString() , arg0.get(0)+"_"+arg0.get(4)+"_"+arg0.get(5));
			}
		}).groupByKey().mapToPair(new PairFunction<Tuple2<String,Iterable<String>>, String, String>() {
			public Tuple2<String, String> call(
					Tuple2<String, Iterable<String>> arg0) throws Exception {
				int total =0;
				int count=0;
				String shop ="";
				for(String value : arg0._2()){
					String [] split = value.split("_");
					total+= Integer.parseInt(split[split.length-1]);
					shop = split[0];
					count++;
				}
				
				String output = shop+"_"+count+"_"+(total/count);
				return new Tuple2<String, String>(arg0._1(),output);
			}
		});
		return userConsumeShop;
	}
	
	private static JavaPairRDD<String, String> getOrderAugustRDD (DataFrame orderDataFrame){
		DataFrame august = orderDataFrame.sqlContext().sql("select * from orderData where datestime >= '20150801000000' and datestime <= '20150831000000'");
	    JavaPairRDD<String, String> userConsumeShop = august.toJavaRDD().mapToPair(new PairFunction<Row, String, String>() {
			public Tuple2<String, String> call(Row arg0) throws Exception {
				return new Tuple2<String, String>(arg0.get(6).toString() , arg0.get(0)+"_"+arg0.get(4)+"_"+arg0.get(5));
			}
		}).groupByKey().mapToPair(new PairFunction<Tuple2<String,Iterable<String>>, String, String>() {
			public Tuple2<String, String> call(
					Tuple2<String, Iterable<String>> arg0) throws Exception {
				int total =0;
				int count=0;
				String shop ="";
				for(String value : arg0._2()){
					String [] split = value.split("_");
					total+= Integer.parseInt(split[split.length-1]);
					shop = split[0];
					count++;
				}
				
				String output = shop+"_"+count+"_"+(total/count);
				return new Tuple2<String, String>(arg0._1(),output);
			}
		});
		return userConsumeShop;
	}
	
	private static JavaPairRDD<String, String> getOrderSeptembertRDD (DataFrame orderDataFrame){
		DataFrame september = orderDataFrame.sqlContext().sql("select * from orderData where datestime >= '20150901000000' and datestime <= '20150931000000'");
	    JavaPairRDD<String, String> userConsumeShop = september.toJavaRDD().mapToPair(new PairFunction<Row, String, String>() {
			public Tuple2<String, String> call(Row arg0) throws Exception {
				return new Tuple2<String, String>(arg0.get(6).toString() , arg0.get(0)+"_"+arg0.get(4)+"_"+arg0.get(5));
			}
		}).groupByKey().mapToPair(new PairFunction<Tuple2<String,Iterable<String>>, String, String>() {
			public Tuple2<String, String> call(
					Tuple2<String, Iterable<String>> arg0) throws Exception {
				int total =0;
				int count=0;
				String shop ="";
				for(String value : arg0._2()){
					String [] split = value.split("_");
					total+= Integer.parseInt(split[split.length-1]);
					shop = split[0];
					count++;
				}
				
				String output = shop+"_"+count+"_"+(total/count);
				return new Tuple2<String, String>(arg0._1(),output);
			}
		});
		return userConsumeShop;
	}
	
	private static int maxRepeating(Object[] objects){
		int count = 0;
        int temp =0;
        for(int i=0;i<objects.length;i++){
            if(count==0){
                temp = (int) objects[i];
                count++;
            }else{
                if(temp==(int)objects[i]){
                	count++;
                }else{
                	count--;
                }
            }
        }
        return temp;
    }


}
