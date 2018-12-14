package com.yieldbook.mortgage.hbase.action;

import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Calendar;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import com.opencsv.CSVParser;
import com.opencsv.CSVReader;
import com.opencsv.CSVWriter;
import com.opencsv.ICSVParser;
import com.yieldbook.mortgage.hbase.bulkimport.FnmaArmLoanColumnEnum;
import com.yieldbook.mortgage.hbase.bulkimport.FnmaLoanColumnEnum;
import com.yieldbook.mortgage.hbase.bulkimport.FnmaModLoanColumnEnum;
import com.yieldbook.mortgage.hbase.operation.BatchOp;

public class FnmaDailyFileParser extends BaseFileParser {

	final static String FNMA_LOAN_MONTHLY = "fnma_loan_monthly";
	final static String FNMA_ARM_LOAN_MONTHLY = "fnma_arm_loan_monthly";
	final static String FNMA_MOD_LOAN_MONTHLY = "fnma_mod_loan_monthly";
	final static byte[] COL_FAM = "m".getBytes();
	final static int LOAN_LENGTH = 35;
	final static int ARM_LOAN_LENGTH = 20;
	final static int MOD_LOAN_LENGTH = 39;

	String inputFileName;
	StringBuilder sb = new StringBuilder();
	BatchOp batchLoan = null;
	BatchOp batchArmLoan = null;
	BatchOp batchModLoan = null;
	CSVWriter loanPen = null;
	CSVWriter armLoanPen = null;
	CSVWriter modLoanPen = null;
	String asOfDate;


	public FnmaDailyFileParser(String inputFileName, String asOfDateStr) {
		super();
		this.inputFileName = inputFileName;
		this.asOfDate = asOfDateStr;

		batchLoan = new BatchOp(FNMA_LOAN_MONTHLY);
		batchArmLoan = new BatchOp(FNMA_ARM_LOAN_MONTHLY);
		batchModLoan = new BatchOp(FNMA_MOD_LOAN_MONTHLY);

		try {
			batchLoan.ConnectToTable();
		} catch (IOException e) {
			e.printStackTrace();
			System.out.println("Cannot conenct to HBase table: "
					+ FNMA_LOAN_MONTHLY);
		}

		try {
			batchArmLoan.ConnectToTable();
		} catch (IOException e) {
			e.printStackTrace();
			System.out.println("Cannot conenct to HBase table: "
					+ FNMA_ARM_LOAN_MONTHLY);
		}

		try {
			batchModLoan.ConnectToTable();
		} catch (IOException e) {
			e.printStackTrace();
			System.out.println("Cannot conenct to HBase table: "
					+ FNMA_MOD_LOAN_MONTHLY);
		}

		try {
			loanPen = new CSVWriter(
					new FileWriter("fnma_loan_daily.csv", true), '|',
					CSVWriter.NO_QUOTE_CHARACTER,
					CSVWriter.DEFAULT_ESCAPE_CHARACTER,
					CSVWriter.DEFAULT_LINE_END);
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	public void printLine(String[] line) {
		if (line.length == 1) {
			System.out.println(line[0]);
		} else {
			for (int i = 0; i < line.length - 2; i++)
				System.out.print(line[i] + "|");

			System.out.println(line[line.length - 1]);
		}// end of else
	}

	private void insertRow(String[] row, long lastChgDate, boolean isArm, boolean isMod) throws IOException {

		Put putLoan = createLoanPut(row, lastChgDate);
		batchLoan.insertPuts(putLoan);

		if (isArm) {
			// row[32] mapping to Amortization_Type
			Put putArmLoan = createArmLoanPut(row, lastChgDate);
			batchArmLoan.insertPuts(putArmLoan);
		}
		
		if (isMod) {
			Put putModLoan = createModLoanPut(row, lastChgDate);
			batchModLoan.insertPuts(putModLoan);
		}
	}

	private void insertLoanFields(TableTypeEnum tableType, long lastChgDate,
			String[] src, String[] des) {

		switch (tableType) {
		case LOAN:
			des[0] = src[6]; // loan_identifier
			des[1] = src[0]; // cusip
			des[2] = src[32]; // prod_type_ind
			des[3] = src[26]; // loan_purpose
			des[4] = src[29]; // occupancy_type
			des[5] = src[28]; // num_units
			des[6] = src[30]; // state
			des[7] = src[24]; // credit_score
			des[8] = src[15]; // orig_loan_term
			des[9] = src[20]; // orig_ltv
			des[10] = src[33]; // prepay_premium_term
			des[11] = src[34]; // io_flag
			des[12] = src[16]; // first_payment_date
			des[13] = src[35]; // first_pi_date
			des[14] = src[19]; // maturity_date
			des[15] = src[10]; // orig_note_rate
			des[16] = src[11]; // note_rate
			des[17] = src[12]; // net_note_rate
			des[18] = src[13]; // orig_loan_size
			des[19] = src[17]; // loan_age
			des[20] = src[18]; // rem_months_to_maturity
			des[21] = src[36]; // months_to_amortize
			des[22] = src[9]; // servicer
			des[23] = src[8]; // seller
			des[24] = lastChgDate + ""; // last_chg_date
			des[25] = src[14]; // current_upb
			des[26] = src[1]; // eff_date
			des[27] = src[23]; // orig_dti
			des[28] = src[25]; // first_time_buyer
			des[29] = src[31]; // ins_percent
			des[30] = src[22]; // num_borrowers
			des[31] = src[21]; // orig_cltv
			des[32] = src[27]; // property_type
			des[33] = src[7]; // tpo_flag
			des[34] = this.asOfDate; // as_of_date
			break;
		case ARM_LOAN:
			des[0] = src[6]; // loan_identifier
			des[1] = src[0]; // cusip
			des[2] = src[37]; // convertible_flag
			des[3] = src[46]; // rate_adjmt_freq
			des[4] = src[47]; // initial_period
			des[5] = src[45]; // next_adjmt_date
			des[6] = src[41]; // lookback
			des[7] = src[38]; // gross_margin
			des[8] = src[39]; // net_margin
			des[9] = src[43]; // net_max_life_rate
			des[10] = src[42]; // max_life_rate
			des[11] = src[48]; // init_cap_up
			des[12] = src[49]; // init_cap_dn
			des[13] = src[50]; // periodic_cap_up
			des[14] = src[51]; // periodic_cap_dn
			des[15] = src[44]; // months_to_adjust
			des[16] = src[40]; // index_num
			des[17] = lastChgDate + ""; // last_chg_date
			des[18] = src[1]; // eff_date
			des[19] = this.asOfDate; // as_of_date
			break;
		case MOD_LOAN:
			des[0] = src[6]; // loan_identifier
			des[1] = src[0]; // cusip
			des[2] = src[1]; // eff_date
			des[3] = lastChgDate + ""; // last_chg_date
			des[4] = src[52]; // days_delinquent
			des[5] = src[53]; // loan_performance_history
			des[6] = src[54]; // mod_date_loan_age
			des[7] = src[55]; // mod_program
			des[8] = src[56]; // mod_type
			des[9] = src[57]; // num_of_mods
			des[10] = src[58]; // tot_capitalized_amt
			des[11] = src[59]; // origin_loan_amt
			des[12] = src[61]; // deferred_upb
			des[13] = src[62]; // rate_step_ind
			des[14] = src[63]; // initial_fixed_per
			des[15] = src[64]; // tot_steps
			des[16] = src[65]; // rem_steps
			des[17] = src[66]; // next_step_rate
			des[18] = src[67]; // terminal_step_rate
			des[19] = src[68]; // terminal_step_date
			des[20] = src[69]; // rate_adj_freq
			des[21] = src[70]; // months_to_adj
			des[22] = src[71]; // next_adj_date
			des[23] = src[72]; // periodic_cap_up
			des[24] = src[73]; // origin_channel
			des[25] = src[74]; // origin_note_rate
			des[26] = src[75]; // origin_upb
			des[27] = src[76]; // origin_loan_term
			des[28] = src[77]; // origin_first_paym_date
			des[29] = src[78]; // origin_maturity_date
			des[30] = src[79]; // origin_ltv
			des[31] = src[80]; // origin_cltv
			des[32] = src[81]; // origin_dti
			des[33] = src[82]; // origin_credit_score
			des[34] = src[83]; // origin_loan_purpose
			des[35] = src[84]; // origin_occupancy_status
			des[36] = src[85]; // origin_product_type
			des[37] = src[86]; // origin_io_flag
			des[38] = this.asOfDate; // as_of_date
			break;
		default:
			throw new AssertionError("Unknown Table Type");

		}
	}

	@Override
	public void parseAndWriteFile() {

		CSVReader reader = null;
		String[] line = null;
		long lastChgDate = Calendar.getInstance().getTimeInMillis();
		boolean isArm = false;
		boolean isMod = false;

		try {
			reader = new CSVReader(new FileReader(inputFileName), '|',
					CSVParser.DEFAULT_QUOTE_CHARACTER,
					ICSVParser.DEFAULT_ESCAPE_CHARACTER,
					CSVReader.DEFAULT_SKIP_LINES,
					ICSVParser.DEFAULT_STRICT_QUOTES,
					ICSVParser.DEFAULT_IGNORE_LEADING_WHITESPACE);

			while ((line = reader.readNext()) != null) {
				if (StringUtils.isEmpty(line[0])
						|| StringUtils.isEmpty(line[6]))
					continue;
				isArm = false;
				isMod = false;
				String[] loanFields = new String[LOAN_LENGTH];
				insertLoanFields(TableTypeEnum.LOAN, lastChgDate, line,
						loanFields);
				loanPen.writeNext(loanFields);

				if (!StringUtils.isEmpty(line[32])
						&& line[32].equalsIgnoreCase("ARM")) {
					// row[32] mapping to Amortization_Type
					isArm = true;
					if (armLoanPen == null) {
						armLoanPen = new CSVWriter(new FileWriter(
								"fnma_arm_loan_daily.csv", true), '|',
								CSVWriter.NO_QUOTE_CHARACTER,
								CSVWriter.DEFAULT_ESCAPE_CHARACTER,
								CSVWriter.DEFAULT_LINE_END);
					}
					String[] loanArmFields = new String[ARM_LOAN_LENGTH];
					insertLoanFields(TableTypeEnum.ARM_LOAN, lastChgDate,
							line, loanArmFields);
					armLoanPen.writeNext(loanArmFields);
				}

				if (!StringUtils.isBlank(line[52])
						||!StringUtils.isBlank(line[53])
						||!StringUtils.isBlank(line[54])
						||!StringUtils.isBlank(line[55])
						|| !StringUtils.isBlank(line[56])
						|| !StringUtils.isBlank(line[57])) {
                    // row[52] mapping to days_delinquent
                    // row[53] mapping to loan_performance_history
                    // row[54] mapping to mod_date_loan_age					
					// row[55] mapping to mod_program
					// row[56] mapping to mod_type
					// row[57] mapping to num_of_mods
					isMod = true;
					if (modLoanPen == null) {
						modLoanPen = new CSVWriter(new FileWriter(
								"fnma_mod_loan_daily.csv", true), '|',
								CSVWriter.NO_QUOTE_CHARACTER,
								CSVWriter.DEFAULT_ESCAPE_CHARACTER,
								CSVWriter.DEFAULT_LINE_END);
					}
					String[] loanModFields = new String[MOD_LOAN_LENGTH];
					insertLoanFields(TableTypeEnum.MOD_LOAN, lastChgDate,
							line, loanModFields);
					modLoanPen.writeNext(loanModFields);
				}

				insertRow(line, lastChgDate, isArm, isMod);

			} // end of While
			if (loanPen != null)
				loanPen.close();
			if (armLoanPen != null)
				armLoanPen.close();
			if (modLoanPen != null)
				modLoanPen.close();
		} catch (IOException e) {
			System.out.println("Cannot parse line + \"" + line + "\"");
			e.printStackTrace();
		}
		batchLoan.submitBatch();
		batchArmLoan.submitBatch();
		batchModLoan.submitBatch();
	}

	public Put createLoanPut(String[] fields, long lastChgDate) {
		Put put = new Put(Bytes.toBytes(fields[6]));

		put.addColumn(COL_FAM, FnmaLoanColumnEnum.CUSIP.getColumnName(),
				fields[0].getBytes());
		put.addColumn(COL_FAM,
				FnmaLoanColumnEnum.PROD_TYPE_IND.getColumnName(),
				fields[32].getBytes());
		put.addColumn(COL_FAM, FnmaLoanColumnEnum.LOAN_PURPOSE.getColumnName(),
				fields[26].getBytes());
		put.addColumn(COL_FAM,
				FnmaLoanColumnEnum.OCCUPANCY_TYPE.getColumnName(),
				fields[29].getBytes());
		put.addColumn(COL_FAM, FnmaLoanColumnEnum.NUM_UNITS.getColumnName(),
				fields[28].getBytes());
		put.addColumn(COL_FAM, FnmaLoanColumnEnum.STATE.getColumnName(),
				fields[30].getBytes());
		put.addColumn(COL_FAM, FnmaLoanColumnEnum.CREDIT_SCORE.getColumnName(),
				fields[24].getBytes());
		put.addColumn(COL_FAM,
				FnmaLoanColumnEnum.ORIG_LOAN_TERM.getColumnName(),
				fields[76].getBytes());
		put.addColumn(COL_FAM, FnmaLoanColumnEnum.ORIG_LTV.getColumnName(),
				fields[79].getBytes());
		put.addColumn(COL_FAM,
				FnmaLoanColumnEnum.PREPAY_PREMIUM_TERM.getColumnName(),
				fields[33].getBytes());
		put.addColumn(COL_FAM, FnmaLoanColumnEnum.IO_FLAG.getColumnName(),
				fields[34].getBytes());
		put.addColumn(COL_FAM,
				FnmaLoanColumnEnum.FIRST_PAYMENT_DATE.getColumnName(),
				fields[16].getBytes());
		put.addColumn(COL_FAM,
				FnmaLoanColumnEnum.FIRST_PI_DATE.getColumnName(),
				fields[35].getBytes());
		put.addColumn(COL_FAM,
				FnmaLoanColumnEnum.MATURITY_DATE.getColumnName(),
				fields[19].getBytes());
		put.addColumn(COL_FAM,
				FnmaLoanColumnEnum.ORIG_NOTE_RATE.getColumnName(),
				fields[10].getBytes());
		put.addColumn(COL_FAM, FnmaLoanColumnEnum.NOTE_RATE.getColumnName(),
				fields[11].getBytes());
		put.addColumn(COL_FAM,
				FnmaLoanColumnEnum.NET_NOTE_RATE.getColumnName(),
				fields[12].getBytes());
		put.addColumn(COL_FAM,
				FnmaLoanColumnEnum.ORIG_LOAN_SIZE.getColumnName(),
				fields[13].getBytes());
		put.addColumn(COL_FAM, FnmaLoanColumnEnum.LOAN_AGE.getColumnName(),
				fields[17].getBytes());
		put.addColumn(COL_FAM,
				FnmaLoanColumnEnum.REM_MONTHS_TO_MATURITY.getColumnName(),
				fields[18].getBytes());
		put.addColumn(COL_FAM,
				FnmaLoanColumnEnum.MONTHS_TO_AMORTIZE.getColumnName(),
				fields[36].getBytes());
		put.addColumn(COL_FAM, FnmaLoanColumnEnum.SERVICER.getColumnName(),
				fields[9].getBytes());
		put.addColumn(COL_FAM, FnmaLoanColumnEnum.SELLER.getColumnName(),
				fields[8].getBytes());
		put.addColumn(COL_FAM, FnmaLoanColumnEnum.CURRENT_UPB.getColumnName(),
				fields[14].getBytes());
		put.addColumn(COL_FAM, FnmaLoanColumnEnum.EFF_DATE.getColumnName(),
				fields[1].getBytes());
		put.addColumn(COL_FAM, FnmaLoanColumnEnum.ORIG_DTI.getColumnName(),
				fields[23].getBytes());
		put.addColumn(COL_FAM,
				FnmaLoanColumnEnum.FIRST_TIME_BUYER.getColumnName(),
				fields[25].getBytes());
		put.addColumn(COL_FAM, FnmaLoanColumnEnum.INS_PERCENT.getColumnName(),
				fields[31].getBytes());
		put.addColumn(COL_FAM,
				FnmaLoanColumnEnum.NUM_BORROWERS.getColumnName(),
				fields[22].getBytes());
		put.addColumn(COL_FAM, FnmaLoanColumnEnum.ORIG_CLTV.getColumnName(),
				fields[21].getBytes());
		put.addColumn(COL_FAM,
				FnmaLoanColumnEnum.PROPERTY_TYPE.getColumnName(),
				fields[27].getBytes());
		put.addColumn(COL_FAM, FnmaLoanColumnEnum.TPO_FLAG.getColumnName(),
				fields[7].getBytes());
		put.addColumn(COL_FAM, FnmaLoanColumnEnum.AS_OF_DATE.getColumnName(),
				this.asOfDate.getBytes());

		String lastChgDateStr = lastChgDate + "";
		put.addColumn(COL_FAM,
				FnmaLoanColumnEnum.LAST_CHG_DATE.getColumnName(),
				lastChgDateStr.getBytes());

		return put;
	}

	public Put createArmLoanPut(String[] fields, long lastChgDate) {
		Put put = new Put(Bytes.toBytes(fields[6]));

		put.addColumn(COL_FAM, FnmaArmLoanColumnEnum.CUSIP.getColumnName(),
				fields[0].getBytes());
		put.addColumn(COL_FAM,
				FnmaArmLoanColumnEnum.CONVERTIBLE_FLAG.getColumnName(),
				fields[37].getBytes());
		put.addColumn(COL_FAM,
				FnmaArmLoanColumnEnum.RATE_ADJMT_FREQ.getColumnName(),
				fields[46].getBytes());
		put.addColumn(COL_FAM,
				FnmaArmLoanColumnEnum.INITIAL_PERIOD.getColumnName(),
				fields[47].getBytes());
		put.addColumn(COL_FAM,
				FnmaArmLoanColumnEnum.NEXT_ADJMT_DATE.getColumnName(),
				fields[45].getBytes());
		put.addColumn(COL_FAM, FnmaArmLoanColumnEnum.LOOKBACK.getColumnName(),
				fields[41].getBytes());
		put.addColumn(COL_FAM,
				FnmaArmLoanColumnEnum.GROSS_MARGIN.getColumnName(),
				fields[38].getBytes());
		put.addColumn(COL_FAM,
				FnmaArmLoanColumnEnum.NET_MARGIN.getColumnName(),
				fields[39].getBytes());
		put.addColumn(COL_FAM,
				FnmaArmLoanColumnEnum.NET_MAX_LIFE_RATE.getColumnName(),
				fields[43].getBytes());
		put.addColumn(COL_FAM,
				FnmaArmLoanColumnEnum.MAX_LIFE_RATE.getColumnName(),
				fields[42].getBytes());
		put.addColumn(COL_FAM,
				FnmaArmLoanColumnEnum.INIT_CAP_UP.getColumnName(),
				fields[48].getBytes());
		put.addColumn(COL_FAM,
				FnmaArmLoanColumnEnum.INIT_CAP_DN.getColumnName(),
				fields[49].getBytes());
		put.addColumn(COL_FAM,
				FnmaArmLoanColumnEnum.PERIODIC_CAP_UP.getColumnName(),
				fields[50].getBytes());
		put.addColumn(COL_FAM,
				FnmaArmLoanColumnEnum.PERIODIC_CAP_DN.getColumnName(),
				fields[51].getBytes());
		put.addColumn(COL_FAM,
				FnmaArmLoanColumnEnum.MONTHS_TO_ADJUST.getColumnName(),
				fields[44].getBytes());
		put.addColumn(COL_FAM, FnmaArmLoanColumnEnum.INDEX_NUM.getColumnName(),
				fields[40].getBytes());
		put.addColumn(COL_FAM, FnmaArmLoanColumnEnum.EFF_DATE.getColumnName(),
				fields[1].getBytes());
		put.addColumn(COL_FAM,
				FnmaArmLoanColumnEnum.AS_OF_DATE.getColumnName(),
				this.asOfDate.getBytes());

		String lastChgDateStr = lastChgDate + "";
		put.addColumn(COL_FAM,
				FnmaArmLoanColumnEnum.LAST_CHG_DATE.getColumnName(),
				lastChgDateStr.getBytes());

		return put;
	}

	public Put createModLoanPut(String[] fields, long lastChgDate) {
		Put put = new Put(Bytes.toBytes(fields[6]));

		put.addColumn(COL_FAM, FnmaModLoanColumnEnum.CUSIP.getColumnName(),
				fields[0].getBytes());
		put.addColumn(COL_FAM, FnmaModLoanColumnEnum.EFF_DATE.getColumnName(),
				fields[1].getBytes());
		put.addColumn(COL_FAM,
				FnmaModLoanColumnEnum.DAYS_DELINQUENT.getColumnName(),
				fields[52].getBytes());
		put.addColumn(COL_FAM,
				FnmaModLoanColumnEnum.LOAN_PERFORMANCE_HISTORY.getColumnName(),
				fields[53].getBytes());
		put.addColumn(COL_FAM,
				FnmaModLoanColumnEnum.MOD_DATE_LOAN_AGE.getColumnName(),
				fields[54].getBytes());
		put.addColumn(COL_FAM,
				FnmaModLoanColumnEnum.MOD_PROGRAM.getColumnName(),
				fields[55].getBytes());
		put.addColumn(COL_FAM, FnmaModLoanColumnEnum.MOD_TYPE.getColumnName(),
				fields[56].getBytes());
		put.addColumn(COL_FAM,
				FnmaModLoanColumnEnum.NUM_OF_MODS.getColumnName(),
				fields[57].getBytes());
		put.addColumn(COL_FAM,
				FnmaModLoanColumnEnum.TOT_CAPITALIZED_AMT.getColumnName(),
				fields[58].getBytes());
		put.addColumn(COL_FAM,
				FnmaModLoanColumnEnum.ORIGIN_LOAN_AMT.getColumnName(),
				fields[59].getBytes());
		put.addColumn(COL_FAM,
				FnmaModLoanColumnEnum.DEFERRED_UPB.getColumnName(),
				fields[61].getBytes());
		put.addColumn(COL_FAM,
				FnmaModLoanColumnEnum.RATE_STEP_IND.getColumnName(),
				fields[62].getBytes());
		put.addColumn(COL_FAM,
				FnmaModLoanColumnEnum.INITIAL_FIXED_PER.getColumnName(),
				fields[63].getBytes());
		put.addColumn(COL_FAM, FnmaModLoanColumnEnum.TOT_STEPS.getColumnName(),
				fields[64].getBytes());
		put.addColumn(COL_FAM, FnmaModLoanColumnEnum.REM_STEPS.getColumnName(),
				fields[65].getBytes());
		put.addColumn(COL_FAM,
				FnmaModLoanColumnEnum.NEXT_STEP_RATE.getColumnName(),
				fields[66].getBytes());
		put.addColumn(COL_FAM,
				FnmaModLoanColumnEnum.TERMINAL_STEP_RATE.getColumnName(),
				fields[67].getBytes());
		put.addColumn(COL_FAM,
				FnmaModLoanColumnEnum.TERMINAL_STEP_DATE.getColumnName(),
				fields[68].getBytes());
		put.addColumn(COL_FAM,
				FnmaModLoanColumnEnum.RATE_ADJ_FREQ.getColumnName(),
				fields[69].getBytes());
		put.addColumn(COL_FAM,
				FnmaModLoanColumnEnum.MONTHS_TO_ADJ.getColumnName(),
				fields[70].getBytes());
		put.addColumn(COL_FAM,
				FnmaModLoanColumnEnum.NEXT_ADJ_DATE.getColumnName(),
				fields[71].getBytes());
		put.addColumn(COL_FAM,
				FnmaModLoanColumnEnum.PERIODIC_CAP_UP.getColumnName(),
				fields[72].getBytes());
		put.addColumn(COL_FAM,
				FnmaModLoanColumnEnum.ORIGIN_CHANNEL.getColumnName(),
				fields[73].getBytes());
		put.addColumn(COL_FAM,
				FnmaModLoanColumnEnum.ORIGIN_NOTE_RATE.getColumnName(),
				fields[74].getBytes());
		put.addColumn(COL_FAM,
				FnmaModLoanColumnEnum.ORIGIN_UPB.getColumnName(),
				fields[75].getBytes());
		put.addColumn(COL_FAM,
				FnmaModLoanColumnEnum.ORIGIN_LOAN_TERM.getColumnName(),
				fields[76].getBytes());
		put.addColumn(COL_FAM,
				FnmaModLoanColumnEnum.ORIGIN_FIRST_PAYM_DATE.getColumnName(),
				fields[77].getBytes());
		put.addColumn(COL_FAM,
				FnmaModLoanColumnEnum.ORIGIN_MATURITY_DATE.getColumnName(),
				fields[78].getBytes());
		put.addColumn(COL_FAM,
				FnmaModLoanColumnEnum.ORIGIN_LTV.getColumnName(),
				fields[79].getBytes());
		put.addColumn(COL_FAM,
				FnmaModLoanColumnEnum.ORIGIN_CLTV.getColumnName(),
				fields[80].getBytes());
		put.addColumn(COL_FAM,
				FnmaModLoanColumnEnum.ORIGIN_DTI.getColumnName(),
				fields[81].getBytes());
		put.addColumn(COL_FAM,
				FnmaModLoanColumnEnum.ORIGIN_CREDIT_SCORE.getColumnName(),
				fields[82].getBytes());
		put.addColumn(COL_FAM,
				FnmaModLoanColumnEnum.ORIGIN_LOAN_PURPOSE.getColumnName(),
				fields[83].getBytes());
		put.addColumn(COL_FAM,
				FnmaModLoanColumnEnum.ORIGIN_OCCUPANCY_STATUS.getColumnName(),
				fields[84].getBytes());
		put.addColumn(COL_FAM,
				FnmaModLoanColumnEnum.ORIGIN_PRODUCT_TYPE.getColumnName(),
				fields[85].getBytes());
		put.addColumn(COL_FAM,
				FnmaModLoanColumnEnum.ORIGIN_IO_FLAG.getColumnName(),
				fields[86].getBytes());
		put.addColumn(COL_FAM,
				FnmaModLoanColumnEnum.AS_OF_DATE.getColumnName(),
				this.asOfDate.getBytes());

		String lastChgDateStr = lastChgDate + "";
		put.addColumn(COL_FAM,
				FnmaModLoanColumnEnum.LAST_CHG_DATE.getColumnName(),
				lastChgDateStr.getBytes());

		return put;
	}
}
