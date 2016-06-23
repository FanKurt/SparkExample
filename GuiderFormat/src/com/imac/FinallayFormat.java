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

public class FinallayFormat {
//	private ArrayList<String> arrayList = new ArrayList<>();
	private ArrayList<String> splitArrayList = new ArrayList<>();
	public FinallayFormat(int file_total) throws IOException {
		
		initSplitList();
		System.out.println("Final Foramt Start...");
		for (int i = 1; i < file_total; i++) {
			FileInputStream fstream = new FileInputStream(
					"D:/蓋德科技-跌倒數據/clean2/" + i + ".txt");
			DataInputStream in = new DataInputStream(fstream);
			BufferedReader br = new BufferedReader(new InputStreamReader(in));
			String strLine;
			while ((strLine = br.readLine()) != null) {
				if (!strLine.equals("x,y,z,speed,time,svm")) {
					splitData(i,strLine);
				}
			}
		}
		
//		for(int i=1 ;i<=arrayList.size() ;i++){
//			splitData(i,arrayList.get(i-1));
//		}
//		
		saveFile();
		System.out.println("Final Foramt Finish...");
	}
	
	private void initSplitList() {
		for(int i=0 ;i<6;i++){
			splitArrayList.add("x,y,z,speed,time,svm"+"\r\n");
		}
	}
	
	//splitArrayList flowtype:  0:run_down  1: left_flow  2:right_flow  3:forward_flow  4:behind_flow  5:vertical_flow
	private void splitData(int i, String strLine){
		String value = splitArrayList.get(i % 6);
		value+=strLine+"\r\n";
		splitArrayList.remove(i % 6);
		splitArrayList.add(i % 6, value);
	}
	
	private void saveFile() throws FileNotFoundException {
		for(int i=0 ; i <splitArrayList.size() ;i++){
			PrintStream out = new PrintStream(new FileOutputStream(
					"D:/蓋德科技-跌倒數據/clean2/final_out" + i + ".txt"));
			out.print(splitArrayList.get(i));
		}
	}
}
