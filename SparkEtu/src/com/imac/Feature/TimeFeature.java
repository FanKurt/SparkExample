package com.imac.Feature;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;

public class TimeFeature {
//	private static DateFormat dateFormat = new SimpleDateFormat("yyyyMMddHHmmss");
	/**
	 * @param dataDate
	 * @return �g��-�g�� (1-7)
	 * @throws ParseException
	 */
	public static String getWeekOfDay(String dataDate) throws ParseException{
		DateFormat dateFormat = new SimpleDateFormat("yyyyMMddHHmmss");
		Calendar calendar =  Calendar.getInstance();
		calendar.setTime(dateFormat.parse(dataDate));
		return calendar.get(Calendar.DAY_OF_WEEK)+"";
	}
	
	/**
	 * 
	 * @param dataDate
	 * @return ��� (1��-����) (0-4)
	 * @throws ParseException
	 */
	public static String getMonth(String dataDate) throws ParseException{
		DateFormat dateFormat = new SimpleDateFormat("yyyyMMddHHmmss");
		Calendar calendar =  Calendar.getInstance();
		calendar.setTime(dateFormat.parse(dataDate));
		return calendar.get(Calendar.MONTH)+"";
	}
	/**
	 * 
	 * @param dataDate
	 * @return �� (1-30)
	 * @throws ParseException
	 */
	public static String getDayOfMonth(String dataDate) throws ParseException{
		DateFormat dateFormat = new SimpleDateFormat("yyyyMMddHHmmss");
		Calendar calendar =  Calendar.getInstance();
		calendar.setTime(dateFormat.parse(dataDate));
		return calendar.get(Calendar.DATE)+"";
	}
	
	/**
	 * 
	 * @param dataDate
	 * @return ���� (0-59)
	 * @throws ParseException
	 */
	public static String getMinute(String dataDate) throws ParseException{
		DateFormat dateFormat = new SimpleDateFormat("yyyyMMddHHmmss");
		Calendar calendar =  Calendar.getInstance();
		calendar.setTime(dateFormat.parse(dataDate));
		return calendar.get(Calendar.MINUTE)+"";
	}
	
	/**
	 * 
	 * @param dataDate
	 * @return ���� (0-59)
	 * @throws ParseException
	 */
	public static String getSecond(String dataDate) throws ParseException{
		DateFormat dateFormat = new SimpleDateFormat("yyyyMMddHHmmss");
		Calendar calendar =  Calendar.getInstance();
		calendar.setTime(dateFormat.parse(dataDate));
		return calendar.get(Calendar.SECOND)+"";
	}
	
	/**
	 * 
	 * @param dataDate
	 * @return �p�� (0-23)
	 * @throws ParseException
	 */
	public static String getHour(String dataDate) throws ParseException{
		DateFormat dateFormat = new SimpleDateFormat("yyyyMMddHHmmss");
		Calendar calendar =  Calendar.getInstance();
		calendar.setTime(dateFormat.parse(dataDate));
		return calendar.get(Calendar.HOUR_OF_DAY)+"";
	}
	
	/**
	 * @return 6-8��
	 * @throws ParseException
	 */
	public static boolean isTrainingTime(String arg0) throws ParseException{
		String date = arg0.split(",")[4];
		char month = date.toCharArray()[5];
		return Integer.parseInt(month+"") >=6 && Integer.parseInt(month+"") <=8;
	}
	/**
	 * @return 9��
	 * @throws ParseException
	 */
	public static boolean isTestingTime(String arg0) throws ParseException{
		String date = arg0.split(",")[4];
		char month = date.toCharArray()[5];
		return Integer.parseInt(month+"") ==9;
	}
	
}
