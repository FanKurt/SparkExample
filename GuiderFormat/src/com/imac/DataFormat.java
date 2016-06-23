package com.imac;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.util.ArrayList;

public class DataFormat {
	private static int IMEI = 0;
	private static int action = 1;
	private static ArrayList<Double> Gensor_X = new ArrayList<>();
	private static ArrayList<Double> Gensor_Y = new ArrayList<>();
	private static ArrayList<Double> Gensor_Z = new ArrayList<>();
	private static ArrayList<Integer> Gensor_Speed = new ArrayList<>();
	private static ArrayList<Double> Gsensor_timestamp = new ArrayList<>();
	private static ArrayList<Double> Gsensor_svm = new ArrayList<>();
	private static String outputString;
	private static boolean boo = false; // data 10/per , IMEI++ IMEI%10 will
										// error
	public static void main(String[] argv) throws IOException {
		outputString = "x,y,z,speed,time,svm" + "\r\n";
		for (int i = 1; i <= 3; i++) {
			try {
//				FileInputStream fstream = new FileInputStream("D:/蓋德科技-跌倒數據/clean data/output-"+ ((i < 10) ? "0" + i : i) + ".txt");
				FileInputStream fstream = new FileInputStream("D:/蓋德科技-跌倒數據/clean data/behind_flow"+ i + ".txt");
				DataInputStream in = new DataInputStream(fstream);
				BufferedReader br = new BufferedReader(new InputStreamReader(in));
				String strLine;
				while ((strLine = br.readLine()) != null) {
//					System.out.println("###  " + strLine);
//					if (strLine.equals("=")) {
//						boo = false;
//						IMEI++;
//						System.out.println("~~~~~~~~~~" + IMEI);
//					}
//					if (!strLine.equals("=") && !strLine.equals("IMEI")) {
//						setArrayValue(strLine);
//					}
//
//					if (IMEI != 0 && IMEI % 10 == 0 && !boo) {
//						boo = true;
//						System.out.println(action + " !!!!");
//						setValue();
//						saveFile(outputString);
//						action++;
//						outputString = "x,y,z,speed,time,svm" + "\r\n";
//						resest();
//					}
					
					if (!strLine.equals("x,y,z,speed,time,svm")) {
						outputString+=strLine+"\r\n";
					}
				}
				System.out.println("Finish!!");
			} catch (Exception e) {
				System.out.print("~~~~~~~~~~~~~~~~~~     "+e);
			}
		}
//		FinallayFormat finallayFormat = new FinallayFormat(action);
		saveFile(outputString);
	}

	private static void resest() {
		Gensor_X.clear();
		Gensor_Y.clear();
		Gensor_Z.clear();
		Gensor_Speed.clear();
		Gsensor_timestamp.clear();
		Gsensor_svm.clear();
	}

	private static void setValue() {
		System.out.println("" + Gensor_X.size());
		System.out.println("" + Gensor_Y.size());
		System.out.println("" + Gensor_Z.size());
		System.out.println("" + Gensor_Speed.size());
		System.out.println("" + Gsensor_timestamp.size());
		System.out.println("" + Gsensor_svm.size());
		for (int i = 0; i < Gensor_Speed.size(); i++) {
			outputString += Gensor_X.get(i) + "," + Gensor_Y.get(i) + ","
					+ Gensor_Z.get(i) + "," + Gensor_Speed.get(i) + ","
					+ Gsensor_timestamp.get(i) + "," + Gsensor_svm.get(i)
					+ "\r\n";
		}
	}

	private static void setArrayValue(String strLine) {
		if (strLine.contains("Gensor_X")) {
			String[] array = strLine.substring(strLine.indexOf("[") + 1,
					strLine.indexOf("]") - 1).split(",");
			for (String value : array) {
				Gensor_X.add(Double.parseDouble(value));
			}
		} else if (strLine.contains("Gensor_Y")) {
			String[] array = strLine.substring(strLine.indexOf("[") + 1,
					strLine.indexOf("]") - 1).split(",");
			for (String value : array) {
				Gensor_Y.add(Double.parseDouble(value));
			}

		} else if (strLine.contains("Gensor_Z")) {
			String[] array = strLine.substring(strLine.indexOf("[") + 1,
					strLine.indexOf("]") - 1).split(",");
			for (String value : array) {
				Gensor_Z.add(Double.parseDouble(value));
			}

		} else if (strLine.contains("Gensor_Speed")) {
			String[] array = strLine.substring(strLine.indexOf("[") + 1,
					strLine.indexOf("]") - 1).split(",");
			for (String value : array) {
				if (!value.trim().equals(""))
					Gensor_Speed.add(Integer.parseInt(value.trim()));
			}

		} else if (strLine.contains("Gsensor_timestamp")) {
			String[] array = strLine.substring(strLine.indexOf("[") + 1,
					strLine.indexOf("]") - 1).split(",");
			for (String value : array) {
				Gsensor_timestamp.add(Double.parseDouble(value));
			}

		} else if (strLine.contains("Gsensor_svm")) {
			String[] array = strLine.substring(strLine.indexOf("[") + 1,
					strLine.indexOf("]") - 1).split(",");
			for (String value : array) {
				Gsensor_svm.add(Double.parseDouble(value));
			}

		}
	}

	private static void saveFile(String str) throws FileNotFoundException {
		PrintStream out = new PrintStream(new FileOutputStream(
				"D:/蓋德科技-跌倒數據/finish_data/behind_flow.txt"));
		out.print(str);
	}

}
