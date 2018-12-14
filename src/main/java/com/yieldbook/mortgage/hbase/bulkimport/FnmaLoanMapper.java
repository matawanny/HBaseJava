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
public class FnmaLoanMapper extends
		Mapper<LongWritable, Text, ImmutableBytesWritable, KeyValue> {
	// Set column family name
	final static byte[] COL_FAM = "m".getBytes();

	CSVParser csvParser = new CSVParser('|');
	String tableName = "";
	String effDate= "";
	String asOfDate="";

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
			context.getCounter("FnmaLoanMapper", "PARSE_ERRORS").increment(1);
			return;
		}

		if (StringUtils.isEmpty(fields[0])) {
			context.getCounter("FnmaLoanMapper", "MISSING_CUSIP").increment(1);
			return;
		}
	
		if (StringUtils.isEmpty(fields[6])) {
			context.getCounter("FnmaLoanMapper", "MISSING_LOAN_IDENTIFIER").increment(1);
			return;
		}
		
		hKey.set(String.format("%s", fields[6]).getBytes());

		if (!StringUtils.isEmpty(fields[0])) {
			kv = new KeyValue(hKey.get(), COL_FAM,
					FnmaLoanColumnEnum.CUSIP.getColumnName(), fields[0].getBytes());
			context.write(hKey, kv);
		}
		

		if (!StringUtils.isEmpty(fields[7])) {
			kv = new KeyValue(hKey.get(), COL_FAM,
					FnmaLoanColumnEnum.TPO_FLAG.getColumnName(), fields[7].getBytes());
			context.write(hKey, kv);
		}

		if (!StringUtils.isEmpty(fields[9])) {
			kv = new KeyValue(hKey.get(), COL_FAM,
					FnmaLoanColumnEnum.SERVICER.getColumnName(), fields[9].getBytes());
			context.write(hKey, kv);
		}
		if (!StringUtils.isEmpty(fields[10])) {
			kv = new KeyValue(hKey.get(), COL_FAM,
					FnmaLoanColumnEnum.ORIG_NOTE_RATE.getColumnName(), fields[10].getBytes());
			context.write(hKey, kv);
		}
		if (!StringUtils.isEmpty(fields[11])) {
			kv = new KeyValue(hKey.get(), COL_FAM,
					FnmaLoanColumnEnum.NOTE_RATE.getColumnName(), fields[11].getBytes());
			context.write(hKey, kv);
		}
		if (!StringUtils.isEmpty(fields[12])) {
			kv = new KeyValue(hKey.get(), COL_FAM,
					FnmaLoanColumnEnum.NET_NOTE_RATE.getColumnName(), fields[12].getBytes());
			context.write(hKey, kv);
		}
		if (!StringUtils.isEmpty(fields[13])) {
			kv = new KeyValue(hKey.get(), COL_FAM,
					FnmaLoanColumnEnum.ORIG_LOAN_SIZE.getColumnName(), fields[13].getBytes());
			context.write(hKey, kv);
		}
		if (!StringUtils.isEmpty(fields[14])) {
			kv = new KeyValue(hKey.get(), COL_FAM,
					FnmaLoanColumnEnum.CURRENT_UPB.getColumnName(), fields[14].getBytes());
			context.write(hKey, kv);
		}
		if (!StringUtils.isEmpty(fields[15])) {
			kv = new KeyValue(hKey.get(), COL_FAM,
					FnmaLoanColumnEnum.ORIG_LOAN_TERM.getColumnName(), fields[76].getBytes());
			context.write(hKey, kv);
		}
		if (!StringUtils.isEmpty(fields[16])) {
			kv = new KeyValue(hKey.get(), COL_FAM,
					FnmaLoanColumnEnum.FIRST_PAYMENT_DATE.getColumnName(), fields[16].getBytes());
			context.write(hKey, kv);
		}
		if (!StringUtils.isEmpty(fields[17])) {
			kv = new KeyValue(hKey.get(), COL_FAM,
					FnmaLoanColumnEnum.LOAN_AGE.getColumnName(), fields[17].getBytes());
			context.write(hKey, kv);
		}
		if (!StringUtils.isEmpty(fields[18])) {
			kv = new KeyValue(hKey.get(), COL_FAM,
					FnmaLoanColumnEnum.REM_MONTHS_TO_MATURITY.getColumnName(), fields[18].getBytes());
			context.write(hKey, kv);
		}
		if (!StringUtils.isEmpty(fields[19])) {
			kv = new KeyValue(hKey.get(), COL_FAM,
					FnmaLoanColumnEnum.MATURITY_DATE.getColumnName(), fields[19].getBytes());
			context.write(hKey, kv);
		}
		if (!StringUtils.isEmpty(fields[20])) {
			kv = new KeyValue(hKey.get(), COL_FAM,
					FnmaLoanColumnEnum.ORIG_LTV.getColumnName(), fields[79].getBytes());
			context.write(hKey, kv);
		}		
		if (!StringUtils.isEmpty(fields[21])) {
			kv = new KeyValue(hKey.get(), COL_FAM,
					FnmaLoanColumnEnum.ORIG_CLTV.getColumnName(), fields[21].getBytes());
			context.write(hKey, kv);
		}
		if (!StringUtils.isEmpty(fields[22])) {
			kv = new KeyValue(hKey.get(), COL_FAM,
					FnmaLoanColumnEnum.NUM_BORROWERS.getColumnName(), fields[22].getBytes());
			context.write(hKey, kv);
		}
		if (!StringUtils.isEmpty(fields[23])) {
			kv = new KeyValue(hKey.get(), COL_FAM,
					FnmaLoanColumnEnum.ORIG_DTI.getColumnName(), fields[23].getBytes());
			context.write(hKey, kv);
		}
		if (!StringUtils.isEmpty(fields[24])) {
			kv = new KeyValue(hKey.get(), COL_FAM,
					FnmaLoanColumnEnum.CREDIT_SCORE.getColumnName(), fields[24].getBytes());
			context.write(hKey, kv);
		}
		if (!StringUtils.isEmpty(fields[25])) {
			kv = new KeyValue(hKey.get(), COL_FAM,
					FnmaLoanColumnEnum.FIRST_TIME_BUYER.getColumnName(), fields[25].getBytes());
			context.write(hKey, kv);
		}
		if (!StringUtils.isEmpty(fields[26])) {
			kv = new KeyValue(hKey.get(), COL_FAM,
					FnmaLoanColumnEnum.LOAN_PURPOSE.getColumnName(), fields[26].getBytes());
			context.write(hKey, kv);
		}
		if (!StringUtils.isEmpty(fields[27])) {
			kv = new KeyValue(hKey.get(), COL_FAM,
					FnmaLoanColumnEnum.PROPERTY_TYPE.getColumnName(), fields[27].getBytes());
			context.write(hKey, kv);
		}
		if (!StringUtils.isEmpty(fields[28])) {
			kv = new KeyValue(hKey.get(), COL_FAM,
					FnmaLoanColumnEnum.NUM_UNITS.getColumnName(), fields[28].getBytes());
			context.write(hKey, kv);
		}
		if (!StringUtils.isEmpty(fields[29])) {
			kv = new KeyValue(hKey.get(), COL_FAM,
					FnmaLoanColumnEnum.OCCUPANCY_TYPE.getColumnName(), fields[29].getBytes());
			context.write(hKey, kv);
		}
		if (!StringUtils.isEmpty(fields[30])) {
			kv = new KeyValue(hKey.get(), COL_FAM,
					FnmaLoanColumnEnum.STATE.getColumnName(), fields[30].getBytes());
			context.write(hKey, kv);
		}
		if (!StringUtils.isEmpty(fields[31])) {
			kv = new KeyValue(hKey.get(), COL_FAM,
					FnmaLoanColumnEnum.INS_PERCENT.getColumnName(), fields[31].getBytes());
			context.write(hKey, kv);
		}
		if (!StringUtils.isEmpty(fields[32])) {
			kv = new KeyValue(hKey.get(), COL_FAM,
					FnmaLoanColumnEnum.PROD_TYPE_IND.getColumnName(), fields[32].getBytes());
			context.write(hKey, kv);
		}
		if (!StringUtils.isEmpty(fields[33])) {
			kv = new KeyValue(hKey.get(), COL_FAM,
					FnmaLoanColumnEnum.PREPAY_PREMIUM_TERM.getColumnName(), fields[33].getBytes());
			context.write(hKey, kv);
		}
		if (!StringUtils.isEmpty(fields[34])) {
			kv = new KeyValue(hKey.get(), COL_FAM,
					FnmaLoanColumnEnum.IO_FLAG.getColumnName(), fields[34].getBytes());
			context.write(hKey, kv);
		}
		if (!StringUtils.isEmpty(fields[35])) {
			kv = new KeyValue(hKey.get(), COL_FAM,
					FnmaLoanColumnEnum.FIRST_PI_DATE.getColumnName(), fields[35].getBytes());
			context.write(hKey, kv);
		}
		if (!StringUtils.isEmpty(fields[36])) {
			kv = new KeyValue(hKey.get(), COL_FAM,
					FnmaLoanColumnEnum.MONTHS_TO_AMORTIZE.getColumnName(), fields[36].getBytes());
			context.write(hKey, kv);
		}

		kv = new KeyValue(hKey.get(), COL_FAM,
				FnmaLoanColumnEnum.EFF_DATE.getColumnName(), this.effDate.getBytes());
		context.write(hKey, kv);
		
		kv = new KeyValue(hKey.get(), COL_FAM,
				FnmaLoanColumnEnum.AS_OF_DATE.getColumnName(), this.asOfDate.getBytes());
		context.write(hKey, kv);		
		
		long lastChgDate = Calendar.getInstance().getTimeInMillis();
		String lastChgDateStr = lastChgDate + "";
		kv = new KeyValue(hKey.get(), COL_FAM,
				FnmaLoanColumnEnum.LAST_CHG_DATE.getColumnName(),
				lastChgDateStr.getBytes());
		context.write(hKey, kv);

		context.getCounter("FnmaLoanMapper", "NUM_RECORDS").increment(1);

	}

}
