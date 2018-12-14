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
public class FnmaModLoanMapper extends
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
			context.getCounter("FnmaModLoanMapper", "PARSE_ERRORS")
					.increment(1);
			return;
		}
		
		if (StringUtils.isEmpty(fields[0])) {
			context.getCounter("FnmaModLoanMapper", "MISSING_CUSIP").increment(1);
			return;
		}
	
		if (StringUtils.isEmpty(fields[6])) {
			context.getCounter("FnmaModLoanMapper", "MISSING_LOAN_IDENTIFIER").increment(1);
			return;
		}		

		if (StringUtils.isBlank(fields[52])    
				&& StringUtils.isBlank(fields[53])
				&& StringUtils.isBlank(fields[54])
				&& StringUtils.isBlank(fields[55])
				&& StringUtils.isBlank(fields[56])
				&& StringUtils.isBlank(fields[57])) {
			// fields[52] mapping to days_delinquent
			// fields[53] mapping to loan_performance_history
			// fields[54] mapping to mod_date_loan_age			
			// fields[55] mapping to mod_program
			// fields[56] mapping to mod_type
			// fields[57] mapping to num_of_mods
			return;
		}

		hKey.set(String.format("%s", fields[6]).getBytes());
		
		if (!StringUtils.isEmpty(fields[0])) {
			kv = new KeyValue(hKey.get(), COL_FAM,
					FnmaModLoanColumnEnum.CUSIP.getColumnName(), fields[0].getBytes());
			context.write(hKey, kv);
		}

		if (!StringUtils.isEmpty(fields[52])) {
			kv = new KeyValue(hKey.get(), COL_FAM,
					FnmaModLoanColumnEnum.DAYS_DELINQUENT.getColumnName(), fields[52].getBytes());
			context.write(hKey, kv);
		}
		if (!StringUtils.isEmpty(fields[53])) {
			kv = new KeyValue(hKey.get(), COL_FAM,
					FnmaModLoanColumnEnum.LOAN_PERFORMANCE_HISTORY.getColumnName(), fields[53].getBytes());
			context.write(hKey, kv);
		}

		if (!StringUtils.isEmpty(fields[55])) {
			kv = new KeyValue(hKey.get(), COL_FAM,
					FnmaModLoanColumnEnum.MOD_PROGRAM.getColumnName(), fields[55].getBytes());
			context.write(hKey, kv);
		}
		if (!StringUtils.isEmpty(fields[56])) {
			kv = new KeyValue(hKey.get(), COL_FAM,
					FnmaModLoanColumnEnum.MOD_TYPE.getColumnName(), fields[56].getBytes());
			context.write(hKey, kv);
		}
		if (!StringUtils.isEmpty(fields[57])) {
			kv = new KeyValue(hKey.get(), COL_FAM,
					FnmaModLoanColumnEnum.NUM_OF_MODS.getColumnName(), fields[57].getBytes());
			context.write(hKey, kv);
		}
		if (!StringUtils.isEmpty(fields[58])) {
			kv = new KeyValue(hKey.get(), COL_FAM,
					FnmaModLoanColumnEnum.TOT_CAPITALIZED_AMT.getColumnName(), fields[58].getBytes());
			context.write(hKey, kv);
		}
		if (!StringUtils.isEmpty(fields[59])) {
			kv = new KeyValue(hKey.get(), COL_FAM,
					FnmaModLoanColumnEnum.ORIGIN_LOAN_AMT.getColumnName(), fields[59].getBytes());
			context.write(hKey, kv);
		}
		if (!StringUtils.isEmpty(fields[61])) {
			kv = new KeyValue(hKey.get(), COL_FAM,
					FnmaModLoanColumnEnum.DEFERRED_UPB.getColumnName(), fields[61].getBytes());
			context.write(hKey, kv);
		}
		if (!StringUtils.isEmpty(fields[62])) {
			kv = new KeyValue(hKey.get(), COL_FAM,
					FnmaModLoanColumnEnum.RATE_STEP_IND.getColumnName(), fields[62].getBytes());
			context.write(hKey, kv);
		}
		if (!StringUtils.isEmpty(fields[63])) {
			kv = new KeyValue(hKey.get(), COL_FAM,
					FnmaModLoanColumnEnum.INITIAL_FIXED_PER.getColumnName(), fields[63].getBytes());
			context.write(hKey, kv);
		}
		if (!StringUtils.isEmpty(fields[64])) {
			kv = new KeyValue(hKey.get(), COL_FAM,
					FnmaModLoanColumnEnum.TOT_STEPS.getColumnName(), fields[64].getBytes());
			context.write(hKey, kv);
		}
		if (!StringUtils.isEmpty(fields[65])) {
			kv = new KeyValue(hKey.get(), COL_FAM,
					FnmaModLoanColumnEnum.REM_STEPS.getColumnName(), fields[65].getBytes());
			context.write(hKey, kv);
		}
		if (!StringUtils.isEmpty(fields[66])) {
			kv = new KeyValue(hKey.get(), COL_FAM,
					FnmaModLoanColumnEnum.NEXT_STEP_RATE.getColumnName(), fields[66].getBytes());
			context.write(hKey, kv);
		}
		if (!StringUtils.isEmpty(fields[67])) {
			kv = new KeyValue(hKey.get(), COL_FAM,
					FnmaModLoanColumnEnum.TERMINAL_STEP_RATE.getColumnName(), fields[67].getBytes());
			context.write(hKey, kv);
		}
		if (!StringUtils.isEmpty(fields[68])) {
			kv = new KeyValue(hKey.get(), COL_FAM,
					FnmaModLoanColumnEnum.TERMINAL_STEP_DATE.getColumnName(), fields[68].getBytes());
			context.write(hKey, kv);
		}
		if (!StringUtils.isEmpty(fields[69])) {
			kv = new KeyValue(hKey.get(), COL_FAM,
					FnmaModLoanColumnEnum.RATE_ADJ_FREQ.getColumnName(), fields[69].getBytes());
			context.write(hKey, kv);
		}
		if (!StringUtils.isEmpty(fields[70])) {
			kv = new KeyValue(hKey.get(), COL_FAM,
					FnmaModLoanColumnEnum.MONTHS_TO_ADJ.getColumnName(), fields[70].getBytes());
			context.write(hKey, kv);
		}
		if (!StringUtils.isEmpty(fields[71])) {
			kv = new KeyValue(hKey.get(), COL_FAM,
					FnmaModLoanColumnEnum.NEXT_ADJ_DATE.getColumnName(), fields[71].getBytes());
			context.write(hKey, kv);
		}
		if (!StringUtils.isEmpty(fields[72])) {
			kv = new KeyValue(hKey.get(), COL_FAM,
					FnmaModLoanColumnEnum.PERIODIC_CAP_UP.getColumnName(), fields[72].getBytes());
			context.write(hKey, kv);
		}
		if (!StringUtils.isEmpty(fields[73])) {
			kv = new KeyValue(hKey.get(), COL_FAM,
					FnmaModLoanColumnEnum.ORIGIN_CHANNEL.getColumnName(), fields[73].getBytes());
			context.write(hKey, kv);
		}
		if (!StringUtils.isEmpty(fields[74])) {
			kv = new KeyValue(hKey.get(), COL_FAM,
					FnmaModLoanColumnEnum.ORIGIN_NOTE_RATE.getColumnName(), fields[74].getBytes());
			context.write(hKey, kv);
		}
		if (!StringUtils.isEmpty(fields[75])) {
			kv = new KeyValue(hKey.get(), COL_FAM,
					FnmaModLoanColumnEnum.ORIGIN_UPB.getColumnName(), fields[75].getBytes());
			context.write(hKey, kv);
		}
		if (!StringUtils.isEmpty(fields[76])) {
			kv = new KeyValue(hKey.get(), COL_FAM,
					FnmaModLoanColumnEnum.ORIGIN_LOAN_TERM.getColumnName(), fields[76].getBytes());
			context.write(hKey, kv);
		}
		if (!StringUtils.isEmpty(fields[77])) {
			kv = new KeyValue(hKey.get(), COL_FAM,
					FnmaModLoanColumnEnum.ORIGIN_FIRST_PAYM_DATE.getColumnName(), fields[77].getBytes());
			context.write(hKey, kv);
		}
		if (!StringUtils.isEmpty(fields[78])) {
			kv = new KeyValue(hKey.get(), COL_FAM,
					FnmaModLoanColumnEnum.ORIGIN_MATURITY_DATE.getColumnName(), fields[78].getBytes());
			context.write(hKey, kv);
		}
		if (!StringUtils.isEmpty(fields[79])) {
			kv = new KeyValue(hKey.get(), COL_FAM,
					FnmaModLoanColumnEnum.ORIGIN_LTV.getColumnName(), fields[79].getBytes());
			context.write(hKey, kv);
		}
		if (!StringUtils.isEmpty(fields[80])) {
			kv = new KeyValue(hKey.get(), COL_FAM,
					FnmaModLoanColumnEnum.ORIGIN_CLTV.getColumnName(), fields[80].getBytes());
			context.write(hKey, kv);
		}
		if (!StringUtils.isEmpty(fields[81])) {
			kv = new KeyValue(hKey.get(), COL_FAM,
					FnmaModLoanColumnEnum.ORIGIN_DTI.getColumnName(), fields[81].getBytes());
			context.write(hKey, kv);
		}
		if (!StringUtils.isEmpty(fields[82])) {
			kv = new KeyValue(hKey.get(), COL_FAM,
					FnmaModLoanColumnEnum.ORIGIN_CREDIT_SCORE.getColumnName(), fields[82].getBytes());
			context.write(hKey, kv);
		}
		if (!StringUtils.isEmpty(fields[83])) {
			kv = new KeyValue(hKey.get(), COL_FAM,
					FnmaModLoanColumnEnum.ORIGIN_LOAN_PURPOSE.getColumnName(), fields[83].getBytes());
			context.write(hKey, kv);
		}
		if (!StringUtils.isEmpty(fields[84])) {
			kv = new KeyValue(hKey.get(), COL_FAM,
					FnmaModLoanColumnEnum.ORIGIN_OCCUPANCY_STATUS.getColumnName(), fields[84].getBytes());
			context.write(hKey, kv);
		}
		if (!StringUtils.isEmpty(fields[85])) {
			kv = new KeyValue(hKey.get(), COL_FAM,
					FnmaModLoanColumnEnum.ORIGIN_PRODUCT_TYPE.getColumnName(), fields[85].getBytes());
			context.write(hKey, kv);
		}
		if (!StringUtils.isEmpty(fields[86])) {
			kv = new KeyValue(hKey.get(), COL_FAM,
					FnmaModLoanColumnEnum.ORIGIN_IO_FLAG.getColumnName(), fields[86].getBytes());
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

		context.getCounter("FnmaModLoanMapper", "NUM_RECORDS").increment(1);

	}

}
