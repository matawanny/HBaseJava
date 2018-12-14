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

/**
 * Mapper Class
 */
public class FnmaArmLoanMapper extends
		Mapper<LongWritable, Text, ImmutableBytesWritable, KeyValue> {
	// Set column family name
	final static byte[] COL_FAM = "m".getBytes();


	CSVParser csvParser = new CSVParser('|');
	String tableName = "";
	String effDate = "";
	String asOfDate = "";

	ImmutableBytesWritable hKey = new ImmutableBytesWritable();
	KeyValue kv;

	/** {@inheritDoc} */
	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		Configuration c = context.getConfiguration();

		// Get parameters
		tableName = c.get("hbase.table.name");
		asOfDate = c.get("as_of_date");
		effDate = asOfDate.substring(0,asOfDate.length()-2).concat("01");
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
			context.getCounter("FnmaArmLoanMapper", "PARSE_ERRORS")
					.increment(1);
			return;
		}
		if (StringUtils.isEmpty(fields[0])) {
			context.getCounter("FnmaArmLoanMapper", "MISSING_CUSIP").increment(1);
			return;
		}
	
		if (StringUtils.isEmpty(fields[6])) {
			context.getCounter("FnmaArmLoanMapper", "MISSING_LOAN_IDENTIFIER").increment(1);
			return;
		}		

		if (!StringUtils.isEmpty(fields[29])
				&& fields[29].equalsIgnoreCase("FRM")) {
			// fields[29] mapping to Amortization_Type
			return;
		}

		hKey.set(String.format("%s", fields[6]).getBytes());
		
		if (!StringUtils.isEmpty(fields[0])) {
			kv = new KeyValue(hKey.get(), COL_FAM,
					FnmaArmLoanColumnEnum.CUSIP.getColumnName(), fields[0].getBytes());
			context.write(hKey, kv);
		}

		if (!StringUtils.isEmpty(fields[37])) {
			kv = new KeyValue(hKey.get(), COL_FAM,
					FnmaArmLoanColumnEnum.CONVERTIBLE_FLAG.getColumnName(), fields[37].getBytes());
			context.write(hKey, kv);
		}
		if (!StringUtils.isEmpty(fields[39])) {
			kv = new KeyValue(hKey.get(), COL_FAM,
					FnmaArmLoanColumnEnum.NET_MARGIN.getColumnName(), fields[39].getBytes());
			context.write(hKey, kv);
		}
		if (!StringUtils.isEmpty(fields[40])) {
			kv = new KeyValue(hKey.get(), COL_FAM,
					FnmaArmLoanColumnEnum.INDEX_NUM.getColumnName(), fields[40].getBytes());
			context.write(hKey, kv);
		}
		if (!StringUtils.isEmpty(fields[41])) {
			kv = new KeyValue(hKey.get(), COL_FAM,
					FnmaArmLoanColumnEnum.LOOKBACK.getColumnName(), fields[41].getBytes());
			context.write(hKey, kv);
		}
		if (!StringUtils.isEmpty(fields[42])) {
			kv = new KeyValue(hKey.get(), COL_FAM,
					FnmaArmLoanColumnEnum.MAX_LIFE_RATE.getColumnName(), fields[42].getBytes());
			context.write(hKey, kv);
		}
		if (!StringUtils.isEmpty(fields[43])) {
			kv = new KeyValue(hKey.get(), COL_FAM,
					FnmaArmLoanColumnEnum.NET_MAX_LIFE_RATE.getColumnName(), fields[43].getBytes());
			context.write(hKey, kv);
		}
		if (!StringUtils.isEmpty(fields[44])) {
			kv = new KeyValue(hKey.get(), COL_FAM,
					FnmaArmLoanColumnEnum.MONTHS_TO_ADJUST.getColumnName(), fields[44].getBytes());
			context.write(hKey, kv);
		}
		if (!StringUtils.isEmpty(fields[45])) {
			kv = new KeyValue(hKey.get(), COL_FAM,
					FnmaArmLoanColumnEnum.NEXT_ADJMT_DATE.getColumnName(), fields[45].getBytes());
			context.write(hKey, kv);
		}
		if (!StringUtils.isEmpty(fields[46])) {
			kv = new KeyValue(hKey.get(), COL_FAM,
					FnmaArmLoanColumnEnum.RATE_ADJMT_FREQ.getColumnName(), fields[46].getBytes());
			context.write(hKey, kv);
		}
		if (!StringUtils.isEmpty(fields[47])) {
			kv = new KeyValue(hKey.get(), COL_FAM,
					FnmaArmLoanColumnEnum.INITIAL_PERIOD.getColumnName(), fields[47].getBytes());
			context.write(hKey, kv);
		}
		if (!StringUtils.isEmpty(fields[48])) {
			kv = new KeyValue(hKey.get(), COL_FAM,
					FnmaArmLoanColumnEnum.INIT_CAP_UP.getColumnName(), fields[48].getBytes());
			context.write(hKey, kv);
		}
		if (!StringUtils.isEmpty(fields[49])) {
			kv = new KeyValue(hKey.get(), COL_FAM,
					FnmaArmLoanColumnEnum.INIT_CAP_DN.getColumnName(), fields[49].getBytes());
			context.write(hKey, kv);
		}
		if (!StringUtils.isEmpty(fields[50])) {
			kv = new KeyValue(hKey.get(), COL_FAM,
					FnmaArmLoanColumnEnum.PERIODIC_CAP_UP.getColumnName(), fields[50].getBytes());
			context.write(hKey, kv);
		}
		if (!StringUtils.isEmpty(fields[51])) {
			kv = new KeyValue(hKey.get(), COL_FAM,
					FnmaArmLoanColumnEnum.PERIODIC_CAP_DN.getColumnName(), fields[51].getBytes());
			context.write(hKey, kv);
		}

		kv = new KeyValue(hKey.get(), COL_FAM,
				FnmaArmLoanColumnEnum.EFF_DATE.getColumnName(), this.effDate.getBytes());
		context.write(hKey, kv);

		kv = new KeyValue(hKey.get(), COL_FAM,
				FnmaArmLoanColumnEnum.AS_OF_DATE.getColumnName(), this.asOfDate.getBytes());
		context.write(hKey, kv);			
		
		long lastChgDate = Calendar.getInstance().getTimeInMillis();
		String lastChgDateStr = lastChgDate + "";
		kv = new KeyValue(hKey.get(), COL_FAM,
				FnmaLoanColumnEnum.LAST_CHG_DATE.getColumnName(),
				lastChgDateStr.getBytes());
		context.write(hKey, kv);

		context.getCounter("FnmaArmLoanMapper", "NUM_RECORDS").increment(1);

	}

}
