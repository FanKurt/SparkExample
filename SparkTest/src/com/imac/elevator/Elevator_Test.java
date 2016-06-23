package com.imac.elevator;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.serializer.KryoSerializer;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.api.java.JavaStreamingContextFactory;
import org.apache.spark.streaming.flume.FlumeUtils;
import org.apache.spark.streaming.flume.SparkFlumeEvent;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public class Elevator_Test {
	String errorTable;
	public static void main(String[] args) throws IOException {
	
		final String ip = args[0];
		final String port = args[1];
		final String checkpointDirectory = "/elevator_recover";

		JavaStreamingContextFactory factory = new JavaStreamingContextFactory() {
			public JavaStreamingContext create() {
				return createContext(ip , port, checkpointDirectory);
			}
		};
		JavaStreamingContext jssc = JavaStreamingContext.getOrCreate(
				checkpointDirectory, factory);
		
		jssc.start();
		jssc.awaitTermination();
	}
	
	protected static JavaStreamingContext createContext(String ip,String port , String checkpointDirectory) {
		
		SparkConf conf = new SparkConf();
		conf.set("spark.serializer", KryoSerializer.class.getName());
		conf.set("es.index.auto.create", "true");
		conf.set("es.nodes", "10.26.1.9:9200");
		conf.set("es.resource", "elevator/status");
		conf.set("es.input.json" , "true");
		conf.setAppName("TestStreaming");
		
		JavaStreamingContext sc = new JavaStreamingContext(conf, Durations.milliseconds(1000));
		JavaReceiverInputDStream<SparkFlumeEvent> flumeStream = FlumeUtils
				.createStream(sc, ip, Integer.parseInt(port));
		JavaDStream<String> flume_data = flumeStream
				.map(new Function<SparkFlumeEvent, String>() {
					public String call(SparkFlumeEvent arg0) throws Exception {
						//取得 Flume 資料
						ByteBuffer bytePayload = arg0.event().getBody();
						String status="";
						String inputData = new String(bytePayload.array());
						if(inputData.contains("info")){
							//解析 JSON資料，取出 data欄位
							JSONObject object = new JSONObject(inputData);
							JSONArray array = object.getJSONObject("info").getJSONArray("data");
							if(array.length()>=12){
								if(array.get(2).toString().trim().equals("1")){
									JSONObject json = new JSONObject();
									isTouch(array , json);
									isElevatorOpen(array , json);
									status = json.toString();
								}
							}
						}
						return status;
					}
				}).filter(new Function<String, Boolean>() {
					public Boolean call(String arg0) throws Exception {
						return !arg0.equals("") ;
					}
				});
		
		flume_data.foreach(new Function<JavaRDD<String>, Void>() {
			public Void call(JavaRDD<String> arg0) throws Exception {
				JavaEsSpark.saveToEs(arg0, "elevator/status");
				return null;
			}
		});
		
		flume_data.print();
		
		return sc;
	}

	//判斷電梯開門
	public static void isElevatorOpen(JSONArray array, JSONObject json) throws JSONException{
		String open_status = array.get(3).toString().trim();
		int level = ((Integer.parseInt(array.get(5).toString()) & (0x3f))+1);
		if(open_status.equals("13") | open_status.equals("45")){
			json.put("Level", level);
		}
	}
	
	private static int BIT_0_COUNT = 0;
	private static int BIT_4_COUNT = 0;
	private static int BIT_5_COUNT = 0;
	private static int BIT_0_ORIGIN = 0;
	private static int BIT_4_ORIGIN = 0;
	private static int BIT_5_ORIGIN = 0;
	//判斷接觸器開關次數
	public static void isTouch(JSONArray array, JSONObject json) throws JSONException{
		int CTL1 = Integer.parseInt(array.get(7).toString()); // 找出 [7]的值
		String CTL1B = String.format("%08d",Integer.parseInt(Integer.toBinaryString(CTL1)));//轉2進制
		String BIT_0 = CTL1B.substring(0, 1);
		String BIT_4 = CTL1B.substring(4, 5);
		String BIT_5 = CTL1B.substring(5, 6);
		
		if(Integer.parseInt(BIT_0) == 1 && BIT_0_ORIGIN==0){
			BIT_0_COUNT++;
		}
		if(Integer.parseInt(BIT_4) == 1 && BIT_4_ORIGIN==0){
			BIT_4_COUNT++;
		}
		if(Integer.parseInt(BIT_5) == 1 && BIT_5_ORIGIN==0){
			BIT_5_COUNT++;
			
		}
		
		BIT_0_ORIGIN = Integer.parseInt(BIT_0);
		BIT_4_ORIGIN = Integer.parseInt(BIT_4);
		BIT_5_ORIGIN = Integer.parseInt(BIT_5);
		json.put("M2", BIT_0_COUNT);
		json.put("M1", BIT_4_COUNT);
		json.put("BK", BIT_5_COUNT);
		json.put("create_time", Elevator_Module.getNowTime());
//		json.put("Level", 0);
	}
}