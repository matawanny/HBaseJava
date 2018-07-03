package com.yieldbook.mortgage.hbase.bulkimport;

import java.io.IOException;
import java.sql.Timestamp;
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
public class FhlmcLoanMapper extends
		Mapper<LongWritable, Text, ImmutableBytesWritable, KeyValue> {
	// Set column family name
	final static byte[] COL_FAM = "m".getBytes();


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
			context.getCounter("FhlmcLoanMapper", "PARSE_ERRORS").increment(1);
			return;
		}

		// Wrong number of fields in a line
		/*		if (fields.length != NUM_FIELDS) {
			context.getCounter("FhlmcLoanMapper", "INVALID_FIELD_LEN").increment(
					1);
			return;
		}*/

		hKey.set(String.format("%s", fields[0]).getBytes());
		
		if (!StringUtils.isEmpty(fields[1])) {
			kv = new KeyValue(hKey.get(), COL_FAM,
					FhlmcLoanColumnEnum.CORRECTION_FLAG.getColumnName(), fields[1].getBytes());
			context.write(hKey, kv);
		}
		if (!StringUtils.isEmpty(fields[2])) {
			kv = new KeyValue(hKey.get(), COL_FAM,
					FhlmcLoanColumnEnum.PREFIX.getColumnName(), fields[2].getBytes());
			context.write(hKey, kv);
		}
		if (!StringUtils.isEmpty(fields[4])) {
			kv = new KeyValue(hKey.get(), COL_FAM,
					FhlmcLoanColumnEnum.CUSIP.getColumnName(), fields[4].getBytes());
			context.write(hKey, kv);
		}
		if (!StringUtils.isEmpty(fields[5])) {
			kv = new KeyValue(hKey.get(), COL_FAM,
					FhlmcLoanColumnEnum.ORIG_LOAN_AMT.getColumnName(), fields[5].getBytes());
			context.write(hKey, kv);
		}
		if (!StringUtils.isEmpty(fields[6])) {
			kv = new KeyValue(hKey.get(), COL_FAM,
					FhlmcLoanColumnEnum.ORIG_UPB.getColumnName(), fields[6].getBytes());
			context.write(hKey, kv);
		}
		if (!StringUtils.isEmpty(fields[7])) {
			kv = new KeyValue(hKey.get(), COL_FAM,
					FhlmcLoanColumnEnum.CURRENT_UPB.getColumnName(), fields[7].getBytes());
			context.write(hKey, kv);
		}
		if (!StringUtils.isEmpty(fields[8])) {
			kv = new KeyValue(hKey.get(), COL_FAM,
					FhlmcLoanColumnEnum.PROD_TYPE_IND.getColumnName(), fields[8].getBytes());
			context.write(hKey, kv);
		}
		if (!StringUtils.isEmpty(fields[9])) {
			kv = new KeyValue(hKey.get(), COL_FAM,
					FhlmcLoanColumnEnum.ORIG_NOTE_RATE.getColumnName(), fields[9].getBytes());
			context.write(hKey, kv);
		}
		if (!StringUtils.isEmpty(fields[12])) {
			kv = new KeyValue(hKey.get(), COL_FAM,
					FhlmcLoanColumnEnum.PC_ISSUANCE_NOTE_RATE.getColumnName(), fields[12].getBytes());
			context.write(hKey, kv);
		}
		if (!StringUtils.isEmpty(fields[13])) {
			kv = new KeyValue(hKey.get(), COL_FAM,
					FhlmcLoanColumnEnum.NET_NOTE_RATE.getColumnName(), fields[13].getBytes());
			context.write(hKey, kv);
		}
		if (!StringUtils.isEmpty(fields[14])) {
			fields[14]=YBTimeDateCurrencyUtilities
				.getMonthYearMillionSecsFHLM(fields[14]);
			kv = new KeyValue(hKey.get(), COL_FAM,
					FhlmcLoanColumnEnum.FIRST_PAYMENT_DATE.getColumnName(), fields[14].getBytes());
			context.write(hKey, kv);
		}
		if (!StringUtils.isEmpty(fields[15])) {
			fields[15]=YBTimeDateCurrencyUtilities
				.getMonthYearMillionSecsFHLM(fields[15]);
			kv = new KeyValue(hKey.get(), COL_FAM,
					FhlmcLoanColumnEnum.MATURITY_DATE.getColumnName(), fields[15].getBytes());
			context.write(hKey, kv);
		}
		if (!StringUtils.isEmpty(fields[16])) {
			kv = new KeyValue(hKey.get(), COL_FAM,
					FhlmcLoanColumnEnum.ORIG_LOAN_TERM.getColumnName(), fields[16].getBytes());
			context.write(hKey, kv);
		}
		if (!StringUtils.isEmpty(fields[17])) {
			kv = new KeyValue(hKey.get(), COL_FAM,
					FhlmcLoanColumnEnum.REM_MONTHS_TO_MATURITY.getColumnName(), fields[17].getBytes());
			context.write(hKey, kv);
		}
		if (!StringUtils.isEmpty(fields[18])) {
			kv = new KeyValue(hKey.get(), COL_FAM,
					FhlmcLoanColumnEnum.LOAN_AGE.getColumnName(), fields[18].getBytes());
			context.write(hKey, kv);
		}
		if (!StringUtils.isEmpty(fields[19])) {
			kv = new KeyValue(hKey.get(), COL_FAM,
					FhlmcLoanColumnEnum.ORIG_LTV.getColumnName(), fields[19].getBytes());
			context.write(hKey, kv);
		}
		if (!StringUtils.isEmpty(fields[20])) {
			kv = new KeyValue(hKey.get(), COL_FAM,
					FhlmcLoanColumnEnum.ORIG_CLTV.getColumnName(), fields[20].getBytes());
			context.write(hKey, kv);
		}
		if (!StringUtils.isEmpty(fields[21])) {
			kv = new KeyValue(hKey.get(), COL_FAM,
					FhlmcLoanColumnEnum.ORIG_DTI.getColumnName(), fields[21].getBytes());
			context.write(hKey, kv);
		}
		if (!StringUtils.isEmpty(fields[22])) {
			kv = new KeyValue(hKey.get(), COL_FAM,
					FhlmcLoanColumnEnum.CREDIT_SCORE.getColumnName(), fields[22].getBytes());
			context.write(hKey, kv);
		}
		if (!StringUtils.isEmpty(fields[26])) {
			kv = new KeyValue(hKey.get(), COL_FAM,
					FhlmcLoanColumnEnum.NUM_BORROWERS.getColumnName(), fields[26].getBytes());
			context.write(hKey, kv);
		}
		if (!StringUtils.isEmpty(fields[27])) {
			kv = new KeyValue(hKey.get(), COL_FAM,
					FhlmcLoanColumnEnum.FIRST_TIME_BUYER.getColumnName(), fields[27].getBytes());
			context.write(hKey, kv);
		}
		if (!StringUtils.isEmpty(fields[28])) {
			kv = new KeyValue(hKey.get(), COL_FAM,
					FhlmcLoanColumnEnum.LOAN_PURPOSE.getColumnName(), fields[28].getBytes());
			context.write(hKey, kv);
		}
		if (!StringUtils.isEmpty(fields[29])) {
			kv = new KeyValue(hKey.get(), COL_FAM,
					FhlmcLoanColumnEnum.OCCUPANCY_STATUS.getColumnName(), fields[29].getBytes());
			context.write(hKey, kv);
		}
		if (!StringUtils.isEmpty(fields[30])) {
			kv = new KeyValue(hKey.get(), COL_FAM,
					FhlmcLoanColumnEnum.NUM_UNITS.getColumnName(), fields[30].getBytes());
			context.write(hKey, kv);
		}
		if (!StringUtils.isEmpty(fields[31])) {
			kv = new KeyValue(hKey.get(), COL_FAM,
					FhlmcLoanColumnEnum.PROPERTY_TYPE.getColumnName(), fields[31].getBytes());
			context.write(hKey, kv);
		}
		if (!StringUtils.isEmpty(fields[32])) {
			kv = new KeyValue(hKey.get(), COL_FAM,
					FhlmcLoanColumnEnum.TPO_FLAG.getColumnName(), fields[32].getBytes());
			context.write(hKey, kv);
		}
		if (!StringUtils.isEmpty(fields[33])) {
			kv = new KeyValue(hKey.get(), COL_FAM,
					FhlmcLoanColumnEnum.STATE.getColumnName(), fields[33].getBytes());
			context.write(hKey, kv);
		}
		if (!StringUtils.isEmpty(fields[34])) {
			kv = new KeyValue(hKey.get(), COL_FAM,
					FhlmcLoanColumnEnum.SELLER.getColumnName(), fields[34].getBytes());
			context.write(hKey, kv);
		}
		if (!StringUtils.isEmpty(fields[35])) {
			kv = new KeyValue(hKey.get(), COL_FAM,
					FhlmcLoanColumnEnum.SERVICER.getColumnName(), fields[35].getBytes());
			context.write(hKey, kv);
		}
		if (!StringUtils.isEmpty(fields[36])) {
			kv = new KeyValue(hKey.get(), COL_FAM,
					FhlmcLoanColumnEnum.INS_PERCENT.getColumnName(), fields[36].getBytes());
			context.write(hKey, kv);
		}
		if (!StringUtils.isEmpty(fields[37])) {
			kv = new KeyValue(hKey.get(), COL_FAM,
					FhlmcLoanColumnEnum.INS_CANCEL_IND.getColumnName(), fields[37].getBytes());
			context.write(hKey, kv);
		}
		if (!StringUtils.isEmpty(fields[38])) {
			kv = new KeyValue(hKey.get(), COL_FAM,
					FhlmcLoanColumnEnum.GOVT_INS_GRNTE.getColumnName(), fields[38].getBytes());
			context.write(hKey, kv);
		}
		if (!StringUtils.isEmpty(fields[39])) {
			kv = new KeyValue(hKey.get(), COL_FAM,
					FhlmcLoanColumnEnum.ASSUMABILITY_IND.getColumnName(), fields[39].getBytes());
			context.write(hKey, kv);
		}
		if (!StringUtils.isEmpty(fields[40])) {
			kv = new KeyValue(hKey.get(), COL_FAM,
					FhlmcLoanColumnEnum.IO_FLAG.getColumnName(), fields[40].getBytes());
			context.write(hKey, kv);
		}
		if (!StringUtils.isEmpty(fields[41])) {
			fields[41]=YBTimeDateCurrencyUtilities
				.getMonthYearMillionSecsFHLM(fields[41]);
			kv = new KeyValue(hKey.get(), COL_FAM,
					FhlmcLoanColumnEnum.FIRST_PI_DATE.getColumnName(), fields[41].getBytes());
			context.write(hKey, kv);
		}
		if (!StringUtils.isEmpty(fields[42])) {
			kv = new KeyValue(hKey.get(), COL_FAM,
					FhlmcLoanColumnEnum.MONTHS_TO_AMORTIZE.getColumnName(), fields[42].getBytes());
			context.write(hKey, kv);
		}
		if (!StringUtils.isEmpty(fields[43])) {
			kv = new KeyValue(hKey.get(), COL_FAM,
					FhlmcLoanColumnEnum.PREPAY_PENALTY_FLAG.getColumnName(), fields[43].getBytes());
			context.write(hKey, kv);
		}
		if (!StringUtils.isEmpty(fields[44])) {
			kv = new KeyValue(hKey.get(), COL_FAM,
					FhlmcLoanColumnEnum.PREPAY_TERM.getColumnName(), fields[44].getBytes());
			context.write(hKey, kv);
		}
		if (!StringUtils.isEmpty(fields[72])) {
			kv = new KeyValue(hKey.get(), COL_FAM,
					FhlmcLoanColumnEnum.ESTM_LTV.getColumnName(), fields[72].getBytes());
			context.write(hKey, kv);
		}
		if (!StringUtils.isEmpty(fields[73])) {
			kv = new KeyValue(hKey.get(), COL_FAM,
					FhlmcLoanColumnEnum.UPD_CREDIT_SCORE.getColumnName(), fields[73].getBytes());
			context.write(hKey, kv);
		}
		if (!StringUtils.isEmpty(fields[105])) {
			kv = new KeyValue(hKey.get(), COL_FAM,
					FhlmcLoanColumnEnum.LAST_CHG_DATE.getColumnName(), fields[105].getBytes());
			context.write(hKey, kv);
		}
		if (!StringUtils.isEmpty(fields[105])) {
			kv = new KeyValue(hKey.get(), COL_FAM,
					FhlmcLoanColumnEnum.EFF_DATE.getColumnName(), fields[105].getBytes());
			context.write(hKey, kv);
		}
		if (!StringUtils.isEmpty(fields[106])) {
			kv = new KeyValue(hKey.get(), COL_FAM,
					FhlmcLoanColumnEnum.AS_OF_DATE.getColumnName(), fields[106].getBytes());
			context.write(hKey, kv);
		}		
			
		long lastChgDate = Calendar.getInstance().getTimeInMillis();
		String lastChgDateStr = lastChgDate+"";
		kv = new KeyValue(hKey.get(), COL_FAM,
					FhlmcLoanColumnEnum.LAST_CHG_DATE.getColumnName(), lastChgDateStr.getBytes());
			context.write(hKey, kv);
		
		context.getCounter("FhlmcLoanMapper", "NUM_RECORDS").increment(1);

	}
	

}
