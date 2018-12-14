package com.yieldbook.mortgage.hbase.bulkimport;

import java.io.IOException;
import java.util.Calendar;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.opencsv.CSVParser;

import static com.yieldbook.mortgage.hbase.utility.YBTimeDateCurrencyUtilities.*;

/**
 * Mapper Class
 */
public class FhlmcArmLoanMapper extends
		Mapper<LongWritable, Text, ImmutableBytesWritable, KeyValue> {
	// Set column family name
	final static byte[] COL_FAM = "m".getBytes();

	CSVParser csvParser = new CSVParser('|');
	String tableName = "";


	ImmutableBytesWritable hKey = new ImmutableBytesWritable();
	KeyValue kv;

	/** {@inheritDoc} */
	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		Configuration c = context.getConfiguration();
		
		
		// Get parameters
		tableName = c.get("hbase.table.name");
		// in parameter is now saved: "pass parameter example"
		// ( passed from Driver class )
	}

	/** {@inheritDoc} */
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		String[] fields = null;

		// Failed to parse line
		try {
			fields = csvParser.parseLine(value.toString());
		} catch (Exception ex) {
			context.getCounter("FhlmcArmLoanMapper", "PARSE_ERRORS").increment(1);
			return;
		}

		if(fields.length<65){
			context.getCounter("FhlmcArmLoanMapper", "PARSE_ERRORS").increment(1);
			System.out.println("java.lang.ArrayIndexOutOfBoundsException, length=" + fields.length);
			System.out.println(value.toString());
			return;
		}		
		
		if (!StringUtils.isEmpty(fields[8])&&fields[8].equalsIgnoreCase("FRM")){
			//fields[8] mapping to Amortization_Type
			return; 
		}		
		hKey.set(String.format("%s", fields[0]).getBytes());

		if (!StringUtils.isEmpty(fields[4])) {
			kv = new KeyValue(hKey.get(), COL_FAM,
					FhlmcArmLoanColumnEnum.CUSIP.getColumnName(), fields[4].getBytes());
			context.write(hKey, kv);
		}
		
		if (!StringUtils.isEmpty(fields[8])) {
			kv = new KeyValue(hKey.get(), COL_FAM,
					FhlmcArmLoanColumnEnum.PRODUCT_TYPE.getColumnName(), fields[8].getBytes());
			context.write(hKey, kv);
		}
		
		if (!StringUtils.isEmpty(fields[45])) {
			kv = new KeyValue(hKey.get(), COL_FAM,
					FhlmcArmLoanColumnEnum.INDEX_DESC.getColumnName(), fields[45].getBytes());
			context.write(hKey, kv);
		}
		if (!StringUtils.isEmpty(fields[46])) {
			kv = new KeyValue(hKey.get(), COL_FAM,
					FhlmcArmLoanColumnEnum.GROSS_MARGIN.getColumnName(), fields[46].getBytes());
			context.write(hKey, kv);
		}
		if (!StringUtils.isEmpty(fields[47])) {
			kv = new KeyValue(hKey.get(), COL_FAM,
					FhlmcArmLoanColumnEnum.NET_MARGIN.getColumnName(), fields[47].getBytes());
			context.write(hKey, kv);
		}
		if (!StringUtils.isEmpty(fields[48])) {
			kv = new KeyValue(hKey.get(), COL_FAM,
					FhlmcArmLoanColumnEnum.RATE_ADJMT_FREQ.getColumnName(), fields[48].getBytes());
			context.write(hKey, kv);
		}
		if (!StringUtils.isEmpty(fields[49])) {
			kv = new KeyValue(hKey.get(), COL_FAM,
					FhlmcArmLoanColumnEnum.LOOKBACK.getColumnName(), fields[49].getBytes());
			context.write(hKey, kv);
		}
		if (!StringUtils.isEmpty(fields[52])) {
			kv = new KeyValue(hKey.get(), COL_FAM,
					FhlmcArmLoanColumnEnum.CONVERTIBLE_FLAG.getColumnName(), fields[52].getBytes());
			context.write(hKey, kv);
		}
		if (!StringUtils.isEmpty(fields[53])) {
			kv = new KeyValue(hKey.get(), COL_FAM,
					FhlmcArmLoanColumnEnum.INITIAL_PERIOD.getColumnName(), fields[53].getBytes());
			context.write(hKey, kv);
		}
		if (!StringUtils.isEmpty(fields[54])) {
			fields[54]=getMonthYearMillionSecsEmbs(fields[54]);
			kv = new KeyValue(hKey.get(), COL_FAM,
					FhlmcArmLoanColumnEnum.NEXT_ADJMT_DATE.getColumnName(), fields[54].getBytes());
			context.write(hKey, kv);
		}
		if (!StringUtils.isEmpty(fields[55])) {
			kv = new KeyValue(hKey.get(), COL_FAM,
					FhlmcArmLoanColumnEnum.MONTHS_TO_ADJUST.getColumnName(), fields[55].getBytes());
			context.write(hKey, kv);
		}
		if (!StringUtils.isEmpty(fields[56])) {
			kv = new KeyValue(hKey.get(), COL_FAM,
					FhlmcArmLoanColumnEnum.MAX_LIFE_RATE.getColumnName(), fields[56].getBytes());
			context.write(hKey, kv);
		}
		if (!StringUtils.isEmpty(fields[57])) {
			kv = new KeyValue(hKey.get(), COL_FAM,
					FhlmcArmLoanColumnEnum.NET_MAX_LIFE_RATE.getColumnName(), fields[57].getBytes());
			context.write(hKey, kv);
		}
		if (!StringUtils.isEmpty(fields[60])) {
			kv = new KeyValue(hKey.get(), COL_FAM,
					FhlmcArmLoanColumnEnum.INIT_CAP_UP.getColumnName(), fields[60].getBytes());
			context.write(hKey, kv);
		}
		if (!StringUtils.isEmpty(fields[61])) {
			kv = new KeyValue(hKey.get(), COL_FAM,
					FhlmcArmLoanColumnEnum.INIT_CAP_DN.getColumnName(), fields[61].getBytes());
			context.write(hKey, kv);
		}
		if (!StringUtils.isEmpty(fields[62])) {
			kv = new KeyValue(hKey.get(), COL_FAM,
					FhlmcArmLoanColumnEnum.PERIODIC_CAP.getColumnName(), fields[62].getBytes());
			context.write(hKey, kv);
		}
		if (!StringUtils.isEmpty(fields[fields.length-2])) {
			kv = new KeyValue(hKey.get(), COL_FAM,
					FhlmcArmLoanColumnEnum.EFF_DATE.getColumnName(), fields[fields.length-2].getBytes());
			context.write(hKey, kv);
		}
		if (!StringUtils.isEmpty(fields[fields.length-1])) {
			kv = new KeyValue(hKey.get(), COL_FAM,
					FhlmcArmLoanColumnEnum.AS_OF_DATE.getColumnName(), fields[fields.length-1].getBytes());
			context.write(hKey, kv);
		}		
		long lastChgDate = Calendar.getInstance().getTimeInMillis();
		String lastChgDateStr = lastChgDate+"";
		kv = new KeyValue(hKey.get(), COL_FAM,
					FhlmcLoanColumnEnum.LAST_CHG_DATE.getColumnName(), lastChgDateStr.getBytes());
			context.write(hKey, kv);
		
		context.getCounter("FhlmcArmLoanMapper", "NUM_RECORDS").increment(1);

	}
	

}
