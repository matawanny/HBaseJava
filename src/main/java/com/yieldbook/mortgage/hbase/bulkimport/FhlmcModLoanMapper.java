package com.yieldbook.mortgage.hbase.bulkimport;

import java.io.IOException;
import java.text.ParseException;
import java.util.Calendar;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.opencsv.CSVParser;
import com.yieldbook.mortgage.hbase.utility.YBTimeDateCurrencyUtilities;

/**
 * Mapper Class
 */
public class FhlmcModLoanMapper extends
		Mapper<LongWritable, Text, ImmutableBytesWritable, KeyValue> {
	// Set column family name
	final static byte[] COL_FAM = "m".getBytes();
	// Number of fields in text file
	final static int NUM_FIELDS = 3;

	CSVParser csvParser = new CSVParser('|');
	String tableName = "";
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
		asOfDate = c.get("as.of.date");
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
			context.getCounter("FhlmcModLoanMapper", "PARSE_ERRORS").increment(1);
			return;
		}
		
		if (StringUtils.isEmpty(fields[64])&&StringUtils.isEmpty(fields[65])
				&&StringUtils.isEmpty(fields[66])){
			//fields[64] mapping to mod_program 
			//fields[65] mapping to mod_type
			//fields[66] mapping to num_of_mods
			return; 
		}

		hKey.set(String.format("%s", fields[0]).getBytes());

		if (!StringUtils.isEmpty(fields[1])) {
			kv = new KeyValue(hKey.get(), COL_FAM,
					FhlmcModLoanColumnEnum.CORRECTION_FLAG.getColumnName(), fields[1].getBytes());
			context.write(hKey, kv);
		}
		if (!StringUtils.isEmpty(fields[4])) {
			kv = new KeyValue(hKey.get(), COL_FAM,
					FhlmcModLoanColumnEnum.CUSIP.getColumnName(), fields[4].getBytes());
			context.write(hKey, kv);
		}
		if (!StringUtils.isEmpty(fields[8])) {
			kv = new KeyValue(hKey.get(), COL_FAM,
					FhlmcModLoanColumnEnum.PRODUCT_TYPE.getColumnName(), fields[8].getBytes());
			context.write(hKey, kv);
		}
		if (!StringUtils.isEmpty(fields[64])) {
			kv = new KeyValue(hKey.get(), COL_FAM,
					FhlmcModLoanColumnEnum.MOD_PROGRAM.getColumnName(), fields[64].getBytes());
			context.write(hKey, kv);
		}
		if (!StringUtils.isEmpty(fields[65])) {
			kv = new KeyValue(hKey.get(), COL_FAM,
					FhlmcModLoanColumnEnum.MOD_TYPE.getColumnName(), fields[65].getBytes());
			context.write(hKey, kv);
		}
		if (!StringUtils.isEmpty(fields[66])) {
			kv = new KeyValue(hKey.get(), COL_FAM,
					FhlmcModLoanColumnEnum.NUM_OF_MODS.getColumnName(), fields[66].getBytes());
			context.write(hKey, kv);
		}
		if (!StringUtils.isEmpty(fields[67])) {
			kv = new KeyValue(hKey.get(), COL_FAM,
					FhlmcModLoanColumnEnum.TOT_CAPITALIZED_AMT.getColumnName(), fields[67].getBytes());
			context.write(hKey, kv);
		}
		if (!StringUtils.isEmpty(fields[68])) {
			kv = new KeyValue(hKey.get(), COL_FAM,
					FhlmcModLoanColumnEnum.INT_BEAR_LOAN_AMT.getColumnName(), fields[68].getBytes());
			context.write(hKey, kv);
		}
		if (!StringUtils.isEmpty(fields[69])) {
			kv = new KeyValue(hKey.get(), COL_FAM,
					FhlmcModLoanColumnEnum.DEFERRED_AMT.getColumnName(), fields[69].getBytes());
			context.write(hKey, kv);
		}
		if (!StringUtils.isEmpty(fields[70])) {
			kv = new KeyValue(hKey.get(), COL_FAM,
					FhlmcModLoanColumnEnum.DEFERRED_UPB.getColumnName(), fields[70].getBytes());
			context.write(hKey, kv);
		}
		if (!StringUtils.isEmpty(fields[71])) {
			kv = new KeyValue(hKey.get(), COL_FAM,
					FhlmcModLoanColumnEnum.MOD_DATE_LOAN_AGE.getColumnName(), fields[71].getBytes());
			context.write(hKey, kv);
		}
		if (!StringUtils.isEmpty(fields[75])) {
			kv = new KeyValue(hKey.get(), COL_FAM,
					FhlmcModLoanColumnEnum.RATE_STEP_IND.getColumnName(), fields[75].getBytes());
			context.write(hKey, kv);
		}
		if (!StringUtils.isEmpty(fields[76])) {
			kv = new KeyValue(hKey.get(), COL_FAM,
					FhlmcModLoanColumnEnum.INITIAL_FIXED_PER.getColumnName(), fields[76].getBytes());
			context.write(hKey, kv);
		}
		if (!StringUtils.isEmpty(fields[77])) {
			kv = new KeyValue(hKey.get(), COL_FAM,
					FhlmcModLoanColumnEnum.TOT_STEPS.getColumnName(), fields[77].getBytes());
			context.write(hKey, kv);
		}
		if (!StringUtils.isEmpty(fields[78])) {
			kv = new KeyValue(hKey.get(), COL_FAM,
					FhlmcModLoanColumnEnum.REM_STEPS.getColumnName(), fields[78].getBytes());
			context.write(hKey, kv);
		}
		if (!StringUtils.isEmpty(fields[79])) {
			kv = new KeyValue(hKey.get(), COL_FAM,
					FhlmcModLoanColumnEnum.NEXT_STEP_RATE.getColumnName(), fields[79].getBytes());
			context.write(hKey, kv);
		}
		if (!StringUtils.isEmpty(fields[80])) {
			kv = new KeyValue(hKey.get(), COL_FAM,
					FhlmcModLoanColumnEnum.TERMINAL_STEP_RATE.getColumnName(), fields[80].getBytes());
			context.write(hKey, kv);
		}
		if (!StringUtils.isEmpty(fields[81])) {
			fields[81]=YBTimeDateCurrencyUtilities
				.getMonthYearMillionSecsFHLM(fields[81]);
			kv = new KeyValue(hKey.get(), COL_FAM,
					FhlmcModLoanColumnEnum.TERMINAL_STEP_DATE.getColumnName(), fields[81].getBytes());
			context.write(hKey, kv);
		}
		if (!StringUtils.isEmpty(fields[82])) {
			kv = new KeyValue(hKey.get(), COL_FAM,
					FhlmcModLoanColumnEnum.RATE_ADJ_FREQ.getColumnName(), fields[82].getBytes());
			context.write(hKey, kv);
		}
		if (!StringUtils.isEmpty(fields[83])) {
			fields[83]=YBTimeDateCurrencyUtilities
				.getMonthYearMillionSecsFHLM(fields[83]);
			kv = new KeyValue(hKey.get(), COL_FAM,
					FhlmcModLoanColumnEnum.NEXT_ADJ_DATE.getColumnName(), fields[83].getBytes());
			context.write(hKey, kv);
		}
		if (!StringUtils.isEmpty(fields[84])) {
			kv = new KeyValue(hKey.get(), COL_FAM,
					FhlmcModLoanColumnEnum.MONTHS_TO_ADJ.getColumnName(), fields[84].getBytes());
			context.write(hKey, kv);
		}
		if (!StringUtils.isEmpty(fields[85])) {
			kv = new KeyValue(hKey.get(), COL_FAM,
					FhlmcModLoanColumnEnum.PERIODIC_CAP_UP.getColumnName(), fields[85].getBytes());
			context.write(hKey, kv);
		}
		if (!StringUtils.isEmpty(fields[86])) {
			kv = new KeyValue(hKey.get(), COL_FAM,
					FhlmcModLoanColumnEnum.ORIGIN_LOAN_AMT.getColumnName(), fields[86].getBytes());
			context.write(hKey, kv);
		}
		if (!StringUtils.isEmpty(fields[87])) {
			kv = new KeyValue(hKey.get(), COL_FAM,
					FhlmcModLoanColumnEnum.ORIGIN_NOTE_RATE.getColumnName(), fields[87].getBytes());
			context.write(hKey, kv);
		}
		if (!StringUtils.isEmpty(fields[88])) {
			kv = new KeyValue(hKey.get(), COL_FAM,
					FhlmcModLoanColumnEnum.ORIGIN_PRODUCT_TYPE.getColumnName(), fields[88].getBytes());
			context.write(hKey, kv);
		}
		if (!StringUtils.isEmpty(fields[89])) {
			kv = new KeyValue(hKey.get(), COL_FAM,
					FhlmcModLoanColumnEnum.ORIGIN_IO_FLAG.getColumnName(), fields[89].getBytes());
			context.write(hKey, kv);
		}
		if (!StringUtils.isEmpty(fields[90])) {
			fields[90]=YBTimeDateCurrencyUtilities
				.getMonthYearMillionSecsFHLM(fields[90]);
			kv = new KeyValue(hKey.get(), COL_FAM,
					FhlmcModLoanColumnEnum.ORIGIN_FIRST_PAYM_DATE.getColumnName(), fields[90].getBytes());
			context.write(hKey, kv);
		}
		if (!StringUtils.isEmpty(fields[91])) {
			fields[91]=YBTimeDateCurrencyUtilities
				.getMonthYearMillionSecsFHLM(fields[91]);
			kv = new KeyValue(hKey.get(), COL_FAM,
					FhlmcModLoanColumnEnum.ORIGIN_MATURITY_DATE.getColumnName(), fields[91].getBytes());
			context.write(hKey, kv);
		}
		if (!StringUtils.isEmpty(fields[92])) {
			kv = new KeyValue(hKey.get(), COL_FAM,
					FhlmcModLoanColumnEnum.ORIGIN_LOAN_TERM.getColumnName(), fields[92].getBytes());
			context.write(hKey, kv);
		}
		if (!StringUtils.isEmpty(fields[93])) {
			kv = new KeyValue(hKey.get(), COL_FAM,
					FhlmcModLoanColumnEnum.ORIGIN_LTV.getColumnName(), fields[93].getBytes());
			context.write(hKey, kv);
		}
		if (!StringUtils.isEmpty(fields[94])) {
			kv = new KeyValue(hKey.get(), COL_FAM,
					FhlmcModLoanColumnEnum.ORIGIN_CLTV.getColumnName(), fields[94].getBytes());
			context.write(hKey, kv);
		}
		if (!StringUtils.isEmpty(fields[95])) {
			kv = new KeyValue(hKey.get(), COL_FAM,
					FhlmcModLoanColumnEnum.ORIGIN_DTI.getColumnName(), fields[95].getBytes());
			context.write(hKey, kv);
		}
		if (!StringUtils.isEmpty(fields[96])) {
			kv = new KeyValue(hKey.get(), COL_FAM,
					FhlmcModLoanColumnEnum.ORIGIN_CREDIT_SCORE.getColumnName(), fields[96].getBytes());
			context.write(hKey, kv);
		}
		if (!StringUtils.isEmpty(fields[100])) {
			kv = new KeyValue(hKey.get(), COL_FAM,
					FhlmcModLoanColumnEnum.ORIGIN_LOAN_PURPOSE.getColumnName(), fields[100].getBytes());
			context.write(hKey, kv);
		}
		if (!StringUtils.isEmpty(fields[101])) {
			kv = new KeyValue(hKey.get(), COL_FAM,
					FhlmcModLoanColumnEnum.ORIGIN_OCCUPANCY_STATUS.getColumnName(), fields[101].getBytes());
			context.write(hKey, kv);
		}
		if (!StringUtils.isEmpty(fields[102])) {
			kv = new KeyValue(hKey.get(), COL_FAM,
					FhlmcModLoanColumnEnum.ORIGIN_TPO_FLAG.getColumnName(), fields[102].getBytes());
			context.write(hKey, kv);
		}
		if (!StringUtils.isEmpty(fields[105])) {
			kv = new KeyValue(hKey.get(), COL_FAM,
					FhlmcModLoanColumnEnum.EFF_DATE.getColumnName(), fields[fields.length-2].getBytes());
			context.write(hKey, kv);
		}
		if (!StringUtils.isEmpty(fields[106])) {
			kv = new KeyValue(hKey.get(), COL_FAM,
					FhlmcModLoanColumnEnum.AS_OF_DATE.getColumnName(), fields[fields.length-1].getBytes());
			context.write(hKey, kv);
		}		
		long lastChgDate = Calendar.getInstance().getTimeInMillis();
		String lastChgDateStr = lastChgDate+"";
		kv = new KeyValue(hKey.get(), COL_FAM,
					FhlmcLoanColumnEnum.LAST_CHG_DATE.getColumnName(), lastChgDateStr.getBytes());
			context.write(hKey, kv);
		
		context.getCounter("FhlmcModLoanMapper", "NUM_RECORDS").increment(1);

	}
	

}
