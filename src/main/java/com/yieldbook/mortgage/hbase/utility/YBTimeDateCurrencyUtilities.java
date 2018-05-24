package com.yieldbook.mortgage.hbase.utility;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class YBTimeDateCurrencyUtilities {

	public static final DateFormat df = new SimpleDateFormat("yyyyMMdd");
	public static final DateFormat dft = new SimpleDateFormat("yyyyMMddhh:mm");
	
	public static final DateFormat df1 = new SimpleDateFormat("MM/dd/yyyy");
	public static final DateFormat dft1 = new SimpleDateFormat("MM/dd/yyyy");	
	
	public static final DateFormat dfmonthyear = new SimpleDateFormat("MMyyyy");
	public static final DateFormat dfmonthyear1 = new SimpleDateFormat("MM/yyyy");	
	public static long getMillionSeconds(String asOfDate) throws ParseException{
		
		Date date = null;
		
		if(asOfDate.contains("/")){
			if(asOfDate.contains(":"))
				date = dft1.parse(asOfDate);
			else
				date = df1.parse(asOfDate);
		}else{
			if(asOfDate.contains(":"))
				date = dft.parse(asOfDate);
			else
				date = df.parse(asOfDate);
		}	
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(date);
		return calendar.getTimeInMillis();
	}
	
	public static long getMonthYearMillionSeconds(String asOfDate) throws ParseException{
		
		Date date = null;
		
		if(asOfDate.contains("/")){
			date = dfmonthyear1.parse(asOfDate);
		}else{
			date = dfmonthyear.parse(asOfDate);
		}	
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(date);
		return calendar.getTimeInMillis();
	}
	
	public static long getMonthYearMillionSecsFHLM(String asOfDate) throws ParseException{
		
		Date date = null;
		date = dfmonthyear.parse(asOfDate);
		
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(date);
		return calendar.getTimeInMillis();
	}
	
}
