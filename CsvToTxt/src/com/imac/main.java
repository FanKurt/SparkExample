package com.imac;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URISyntaxException;
import java.net.URL;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.BooleanWritable;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.select.Elements;

import scala.Tuple2;

public class main {

	static String[] array = { "Action", "Adventure	", "Animation",
			"Children.s", "Comedy", "Crime", "Documentary", "Drama", "Fantasy",
			"Film.Noir", "Horror", "Musical", "Mystery", "Romance", "Sci.Fi",
			"Thriller", "War", "Western" };
	static boolean boo = false;
	private static DateFormat dateFormat = new SimpleDateFormat(
			"yyyyMMddHHmmss");
	static SimpleDateFormat dateFormatGmt = new SimpleDateFormat(
			"EE MMM dd HH:mm:ss zzz yyyy", Locale.US);

	public static void main(String[] args) throws IOException, JSONException,
			URISyntaxException, ParseException {
		// read file
		FileInputStream fstream = new FileInputStream("./data/error_table");

		DataInputStream in = new DataInputStream(fstream);
		BufferedReader br = new BufferedReader(new InputStreamReader(in));
		String strLine;
		String outputString = "";
		while ((strLine = br.readLine()) != null) {
			outputString += strLine;
		}
		
		
		JSONObject jsonObject = new JSONObject(outputString);
		double[] mDouble = new double[9];
		mDouble[1] = Double.parseDouble(jsonObject.get("pgmap").toString());
		mDouble[2] = Double.parseDouble(jsonObject.get("pgs").toString());
		mDouble[3] = Double.parseDouble(jsonObject.get("data").toString());
		System.out.println(mDouble[1] + " " + mDouble[2] + " " + mDouble[3]);
		JSONArray array = new JSONArray(outputString);
		System.out.println(array.get(0).toString());
		JSONObject object = new JSONObject(outputString);
		System.out.println(object.get("email"));

		in.close();

		System.out.print("Finished "
				+ "Logistic regression models are neat".substring(5));
		String s = "111.251.190.99 - - [13/Dec/2015:06:57:47 +0800] \"GET /project/instances/ HTTP/1.1\" 302 364 \"http://imac-cloud.nutc.edu.tw/auth/login/?next=/project/instances/\" \"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/47.0.2526.80 Safari/537.36\"";
		String s1 = "176.123.7.171 - - [14/Dec/2015:13:59:30 +0800] \"GET /auth/login/?next=/ HTTP/1.1\" 200 3089 \"-\" \"Mozilla/5.0 (Windows NT 5.1; rv:9.0.1) Gecko/20100101 Firefox/9.0.1\"";
		String s2 = "66.249.85.204 - - [13/Dec/2015:19:26:26 +0800] \"GET /auth/login/?next=/admin/ HTTP/1.1\" 200 3147 \"-\" \"Mozilla/5.0 (Windows NT 6.1; rv:6.0) Gecko/20110814 Firefox/6.0 Google Favicon\"";
		String s3 = "2015-12-14 10:58:03.387 3490 INFO nova.osapi_compute.wsgi.server [req-1a202b70-1b00-493b-9e92-30becfbf76d0 353cfea90fb6465d9cabc0c6488a3639 5f39d1241fc6442784c8e1f6f537bca9 - - -] 10.0.0.11 \"GET /v2/5f39d1241fc6442784c8e1f6f537bca9/servers/detail?all_tenants=True&changes-since=2015-12-14T02%3A57%3A03.616522%2B00%3A00 HTTP/1.1\" status: 200 len: 211 time: 0.0717790";
		String error1 = "[Sat Dec 12 22:57:54.253218 2015] [:error] [pid 23567:tid 140246406543104] RemovedInDjango19Warning: The django.forms.util module has been renamed. Use django.forms.utils instead.";
		String error2 = "[Sat Dec 12 22:57:54.253298 2015] [:error] [pid 23567:tid 140246406543104] WARNING:py.warnings:RemovedInDjango19Warning: The django.forms.util module has been renamed. Use django.forms.utils instead.";
		
		
		Matcher m = horizon_error_log.matcher(error2);
		if (m.find()) {
			for (int i = 0; i < m.groupCount(); i++) {
				System.out.println(i + " 	" + m.group(i));
			}

			URL url = new URL(
					"http://docs.openstack.org/developer/nova/api/nova.exception.html");
			Document xmlDoc = Jsoup.parse(url, 10000); // 使用Jsoup jar 去解析網頁

			Elements title = xmlDoc.select("dl");
			// Elements happy = xmlDoc.select("td");
			String outString = "";
			for (int i = 0; i < title.size(); i = i + 2) {
				try {
					String str1 = title.get(i).select("dt").attr("id");
					String str2 = title.get(i).select("a").get(1).attr("title");
					String str3 = title.get(i).select("em").get(3).html();
					String str3_reg = str3.substring(str3.indexOf("'") + 1,
							str3.lastIndexOf("'"));
					outString += "0," + str1 + " " + str2 + " " + str3_reg
							+ "\n";
					System.out.println("------------------------------");
				} catch (Exception e) {
					String str1 = title.get(i).select("dt").attr("id");
					String str2 = title.get(i).select("dt").select("a")
							.attr("title");
					String str3 = title.get(i).select("dt").select("em").get(0)
							.html();
					outString += "0," + str1 + " " + str2 + " " + str3 + "\n";
					System.out.println("------------------------------");
				}
			}
		}

	}

	public static double[] exchange_sort(double[] ori_arr, boolean isIncrease) {
		double[] arr = ori_arr.clone();
		double len = arr.length;
		for (int i = 0; i < len - 1; i++) {
			for (int k = i + 1; k < len; k++) {
				if ((isIncrease && arr[i] > arr[k])
						|| (!isIncrease && arr[i] < arr[k])) {
					double buffer = arr[i];
					arr[i] = arr[k];
					arr[k] = buffer;
				}
			}
		}
		return arr;
	}

	public static ArrayList<Tuple2<Integer, String>> parseCatFeatures(
			List<String> list) {
		ArrayList<Tuple2<Integer, String>> arrayList = new ArrayList<>();
		for (int i = 0; i < list.size(); i++) {
			arrayList.add(new Tuple2<Integer, String>(i, list.get(i)));
		}

		return arrayList;
	}

	public static final Pattern commend = Pattern.compile("([0-9])()");

	public static final Pattern log = Pattern
			.compile("([(\\d*).]*) (- -) (\\[[\\S*\\/]*) (\\S*\\]) (\\S*) (.*) (\\S*\\/\\S*) (\\d*) (\\d*) (\"\\S*) (\"\\S*) (\\(.*) (\\S*) (\\(\\S*, \\S* \\S*) (\\S*) ((\\S*))");
	public static final Pattern log2 = Pattern
			.compile("([(\\d*).]*) (- -) (\\[[\\S*\\/]*) (\\S*\\]) (\\S*) (.*) (\\S*\\/\\S*) (\\d*) (\\d*) (\\S*) (\\S*) (.*\\)) (\\S*) (\\S*)");

	// 2015-12-14 10:58:03.387 3490 INFO nova.osapi_compute.wsgi.server
	// [req-1a202b70-1b00-493b-9e92-30becfbf76d0
	// 353cfea90fb6465d9cabc0c6488a3639 5f39d1241fc6442784c8e1f6f537bca9 - - -]
	// 10.0.0.11 \"GET
	// /v2/5f39d1241fc6442784c8e1f6f537bca9/servers/detail?all_tenants=True&changes-since=2015-12-14T02%3A57%3A03.616522%2B00%3A00
	// HTTP/1.1\" status: 200 len: 211 time: 0.0717790
	public static final Pattern nova_log = Pattern
			.compile("([(\\d*)-]*) ([\\d*:]*\\.\\d*) (\\d*) ([A-Z]*) (\\S*) (\\[)(\\S*) (\\S*) (\\S*) ([\\S ]*-]) ([\\d*\\.]*\\d*) (\"\\S*) (\\S*)");

	// [Sat Dec 12 22:57:55.870994 2015] [:error] [pid 23567:tid
	// 140246406543104] INFO:muranodashboard.common.cache:Using apps cache
	// directory located at /tmp/muranodashboard-cache/apps
	public static final Pattern horizon_error_log = Pattern
			.compile("(\\w* \\w* \\d*) (\\S*) (\\d*)] \\S* \\[\\w* (\\d*):\\w* (\\d*)] ((.*))");

	// 2016-03-17 15:23:46.299 3183 INFO nova.compute.resource_tracker
	// [req-253a142b-351c-410c-8906-d242139b58bc - - - - -] Auditing locally
	// available compute resources for node compute-ex1
	public static final Pattern nova_compute_log = Pattern
			.compile("([\\d*-]*) ([\\d*:]*.\\d*) (\\d*) (\\w*) ([\\w*\\.]*) \\[([\\w*-]*).*\\] (.*)(\\s*)");

	// 2016-03-19 18:32:22.218 3120 INFO nova.compute.manager
	// [req-83d33c59-43de-4517-9e51-395121d03198
	// 8657e8ede2b44c67801158a8bd9fba89 e25f508f6d534b3b82368b272eb0b104 - - -]
	// [instance: 72c232e8-b239-4c55-9d2d-b71ed32fe0bd] Terminating instance
	public static final Pattern nova_compute_log2 = Pattern
			.compile("([\\d*-]*) ([\\d*:]*.\\d*) (\\d*) (\\w*) ([\\w*\\.]*) \\[([\\w*-]*) ([a-zA-Z0-9]*) ([a-zA-Z0-9]*).*-\\] \\[\\w*\\W\\s(.*)\\] (.*)(\\s*)");

	// 2016-03-18 05:37:15.352 6225 INFO eventlet.wsgi.server [-]
	// 172.16.1.178,<local> - - [18/Mar/2016 05:37:15]
	// "GET /openstack/2013-10-17/user_data HTTP/1.1" 404 176 0.229529
	public static final Pattern neutron_metadata_agent_log = Pattern
			.compile("([\\d*-]*) ([\\d*:]*.\\d*) (\\d*) (\\w*) ([\\w*\\.]*) \\[-\\] ([\\d*\\.]*),.* \"([A-Z]*) (.*)\" (\\d*) (\\d*) (.*)(\\s*)");

	// 2016-03-17 14:28:04.412 2980 INFO neutron.agent.securitygroups_rpc
	// [req-f229ade7-30d6-446e-969a-67a5a407cf20 - - - - -] Refresh firewall
	// rules
	public static final Pattern openvswitch_agent_log = Pattern // 同nova_compute_log
			.compile("([\\d*-]*) ([\\d*:]*.\\d*) (\\d*) (\\w*) ([\\w*\\.]*) \\[([\\w*-]*).*\\] (.*)(\\s*)");

	public static final Pattern ceilometer_error_log = Pattern
			.compile("([\\d*-]*) ([\\d*:]*.\\d*) (\\d*) ([A-Z]*) ([\\w*\\.]*.) (.*) (\\S)");

	public static String bytesToHex(byte[] b) {

		StringBuffer sb = new StringBuffer();
		String stmp = "";
		for (int n = 0; n < b.length; n++) {
			stmp = (Integer.toHexString(b[n] & 0XFF));
			if (stmp.length() == 1) {
				sb.append("0").append(stmp);
			} else {
				sb.append(stmp);
			}
			if (n < b.length - 1) {
				sb.append(":");
			}
		}
		return sb.toString().toUpperCase();
	}

	public static ArrayList removeDuplicateWithOrder(ArrayList arlList) {
		Set set = new HashSet();
		List newList = new ArrayList();
		for (Iterator iter = arlList.iterator(); iter.hasNext();) {
			Object element = iter.next();
			System.out.println(set.add(element));
			if (set.add(element))
				newList.add(element);
		}
		arlList.clear();
		arlList.addAll(newList);
		return arlList;
	}

	public static class Student implements Comparable<Student> {
		String id;
		BooleanWritable boo = new BooleanWritable();

		Student(String b, boolean c) {
			id = b;
			boo.set(c);
		}

		public String getID() {
			return id;
		}

		public BooleanWritable getBoo() {
			return boo;
		}

		public int compareTo(Student other) {
			int ret = -1 * boo.compareTo(other.boo);
			if (ret == 0) {
				ret = id.compareTo(other.id);
			}
			return ret;
		}
	}

	private static String resetTime(String time) {
		int number = 0;
		if (time.indexOf("0") == 0) {
			number = Integer.parseInt(time.substring(1, time.length())) + 8;
		} else {
			number = Integer.parseInt(time) + 8;
		}
		if (number > 24)
			number = number - 24;
		return (number < 10) ? "0" + number : number + "";
	}

	private static String tranferMonth(String month) {
		String[] arrStrings = { "Jan", "Feb", "Mar", "Apr", "Mar", "Jun",
				"Jul", "Aug", "Sep", "Oct", "Nov", "Dec" };
		for (int i = 0; i < arrStrings.length; i++) {
			if (month.equals(arrStrings[i])) {
				return (i < 9) ? "0" + (i + 1) : "" + (i + 1);
			}
		}
		return "";
	}

	private static String getNowTime() {
		Date now = new Date();
		String[] token = now.toString().split(" ");
		String time = token[token.length - 1] + "-" + tranferMonth(token[1])
				+ "-" + token[2] + "T" + token[3] + "Z";
		return time;
	}

	public static final Pattern apacheLogRegex = Pattern
			.compile("(<\\w) (\\S*=)(\"\\w*\")>(([a-zA-Z]* )*[A-Z]*!+)</\\w>");

	public static int numberOfString(String str, String word) {
		if (str.endsWith(word)) {
			return str.split(word).length;
		} else {
			return str.split(word).length - 1;
		}
	}

	static int index = 0;

	public static int indexOfString(String str, String word) {
		int num = str.indexOf(word);
		if (num != -1) {
			String subStr = str.substring(num + 1);
			index++;
			indexOfString(subStr, word);
		}
		return index;
	}

	private static String getMinute(String dataDate) throws ParseException {
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(dateFormat.parse(dataDate));
		return calendar.get(Calendar.DATE) + "";
	}

	private static int maxRepeating(Object[] objects) {
		int count = 0;
		int temp = 0;
		for (int i = 0; i < objects.length; i++) {
			if (count == 0) {
				temp = (int) objects[i];
				count++;
			} else {
				if (temp == (int) objects[i]) {
					count++;
				} else {
					count--;
				}
			}
		}
		return temp;
	}

}
