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
import com.yieldbook.mortgage.hbase.bulkimport.GnmaArmLoanColumnEnum;
import com.yieldbook.mortgage.hbase.bulkimport.GnmaLoanColumnEnum;
import com.yieldbook.mortgage.hbase.operation.BatchOp;

public class GnmaDailyFileParser extends BaseFileParser {

	final static String GNMA_LOAN_MONTHLY = "gnma_loan_monthly";
	final static String GNMA_ARM_LOAN_MONTHLY = "gnma_arm_loan_monthly";
	final static byte[] COL_FAM = "m".getBytes();
	final static int LOAN_LENGTH = 39;
	final static int ARM_LOAN_LENGTH = 15;


	String inputFileName;
	StringBuilder sb = new StringBuilder();
	BatchOp batchLoan = null;
	BatchOp batchArmLoan = null;
	CSVWriter loanPen = null;
	CSVWriter armLoanPen = null;
	String asOfDate;
	String lastChgDate;

	public GnmaDailyFileParser(String inputFileName, String asOfDateStr, String lastChgDateStr) {
		super();
		this.inputFileName = inputFileName;
		this.asOfDate = asOfDateStr;
		this.lastChgDate = lastChgDateStr;
		batchLoan = new BatchOp(GNMA_LOAN_MONTHLY);
		batchArmLoan = new BatchOp(GNMA_ARM_LOAN_MONTHLY);

/*		try {
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
		}*/


		try {
			loanPen = new CSVWriter(
					new FileWriter("gnma_loan_daily.csv", true), '|',
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

	private void insertRow(String[] row, long lastChgDate) throws IOException {

		Put putLoan = createLoanPut(row, lastChgDate);
		batchLoan.insertPuts(putLoan);

		if (!StringUtils.isEmpty(row[32]) && row[32].equalsIgnoreCase("ARM")) {
			// row[32] mapping to Amortization_Type
			Put putArmLoan = createArmLoanPut(row, lastChgDate);
			batchArmLoan.insertPuts(putArmLoan);
		}
		
	}

	private void insertLoanFields(TableTypeEnum tableType, long lastChgDate,
			String[] src, String[] des) {

		switch (tableType) {
		case LOAN:
			des[0]=src[1]; //cusip
			des[1]=src[34]; //eff_date
			des[2]=src[0]; //loan_seq_num
			des[3]=src[2]; //gnma_issuer_num
			des[4]=src[3]; //agency
			des[5]=src[4]; //loan_purpose
			des[6]=src[5]; //refi_type
			des[7]=src[6]; //first_payment_date
			des[8]=src[7]; //maturity_date
			des[9]=src[8]; //note_rate
			des[10]=src[9]; //orig_loan_amt
			des[11]=src[10]; //orig_upb
			des[12]=src[11]; //current_upb
			des[13]=src[12]; //orig_loan_term
			des[14]=src[13]; //loan_age
			des[15]=src[14]; //rem_months_to_maturity
			des[16]=src[15]; //months_delinq
			des[17]=src[16]; //months_prepaid
			des[18]=src[17]; //gross_margin
			des[19]=src[18]; //orig_ltv
			des[20]=src[19]; //orig_cltv
			des[21]=src[20]; //orig_dti
			des[22]=src[21]; //credit_score
			des[23]=src[22]; //down_paym_assist
			des[24]=src[23]; //buy_down_status
			des[25]=src[24]; //upfront_mip
			des[26]=src[25]; //annual_mip
			des[27]=src[26]; //num_borrowers
			des[28]=src[27]; //first_time_buyer
			des[29]=src[28]; //num_units
			des[30]=src[29]; //state
			des[31]=src[30]; //msa
			des[32]=src[31]; //tpo_flag
			des[33]=src[32]; //curr_month_liq_flag
			des[34]=src[33]; //removal_reason
			des[35]=this.lastChgDate; //lastChgDate
			des[36]=src[35]; //loan_orig_date
			des[37]=src[36]; //seller_issuer_id
			des[38]=this.asOfDate; //as_of_date
			break;
		case ARM_LOAN:
			des[0]=src[1]; //cusip
			des[1]=src[0]; //loan_seq_num
			des[2]=src[37]; //index_type
			des[3]=src[38]; //lookback
			des[4]=src[39]; //interest_chg_date
			des[5]=src[40]; //init_rate_cap
			des[6]=src[41]; //sub_init_rate_cap
			des[7]=src[42]; //life_time_rate_cap
			des[8]=src[43]; //next_interest_rate_ceiling
			des[9]=src[44]; //life_time_rate_ceiling
			des[10]=src[45]; //life_time_rate_floor
			des[11]=src[46]; //prospect_interest_rate
			des[12]=this.lastChgDate; //lastChgDate
			des[13]=src[34]; //eff_date
			des[14]=this.asOfDate; //as_of_date
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

		try {
			reader = new CSVReader(new FileReader(inputFileName), '|',
					CSVParser.DEFAULT_QUOTE_CHARACTER,
					ICSVParser.DEFAULT_ESCAPE_CHARACTER,
					CSVReader.DEFAULT_SKIP_LINES,
					ICSVParser.DEFAULT_STRICT_QUOTES,
					ICSVParser.DEFAULT_IGNORE_LEADING_WHITESPACE);

			while ((line = reader.readNext()) != null) {
				if (StringUtils.isEmpty(line[0])
						|| StringUtils.isEmpty(line[1]))
					continue;

				String[] loanFields = new String[LOAN_LENGTH];
				insertLoanFields(TableTypeEnum.LOAN, lastChgDate, line,
						loanFields);
				loanPen.writeNext(loanFields);
				
				if (!StringUtils.isBlank(line[37])    
						||!StringUtils.isBlank(line[38])
						||!StringUtils.isBlank(line[39])
						||!StringUtils.isBlank(line[40])
						|| !StringUtils.isBlank(line[41])
						|| !StringUtils.isBlank(line[42])
						||!StringUtils.isBlank(line[43])
						||!StringUtils.isBlank(line[44])
						|| !StringUtils.isBlank(line[45])
						|| !StringUtils.isBlank(line[46])) {
					// line[37] mapping to indexType
					// line[38] mapping to lookbackPeriod
					// line[39] mapping to interestRateChangeDate
					// line[40] mapping to initialInterestRateCap
					// line[41] mapping to subsequentInterestRateCap
					// line[42] mapping to lifetimeInterestRateCap
					// line[43] mapping to nextInterestRateChangeCeiling
					// line[44] mapping to lifetimeInterestRateCeiling
					// line[45] mapping to lifetimeInterestRateFloor
					// line[46] mapping to prospectiveInterestRate
					if (armLoanPen == null) {
						armLoanPen = new CSVWriter(new FileWriter(
								"gnma_arm_loan_daily.csv", true), '|',
								CSVWriter.NO_QUOTE_CHARACTER,
								CSVWriter.DEFAULT_ESCAPE_CHARACTER,
								CSVWriter.DEFAULT_LINE_END);
					}
					String[] loanArmFields = new String[ARM_LOAN_LENGTH];
					insertLoanFields(TableTypeEnum.ARM_LOAN, lastChgDate,
							line, loanArmFields);
					armLoanPen.writeNext(loanArmFields);
				}


				//insertRow(line, lastChgDate);

			} // end of While
			if (loanPen != null)
				loanPen.close();
			if (armLoanPen != null)
				armLoanPen.close();

		} catch (IOException e) {
			System.out.println("Cannot parse line + \"" + line + "\"");
			e.printStackTrace();
		}
		//batchLoan.submitBatch();
		//batchArmLoan.submitBatch();
	}

	public Put createLoanPut(String[] fields, long lastChgDate) {
		Put put=new Put(Bytes.toBytes(fields[0]));
		put.addColumn(COL_FAM, GnmaLoanColumnEnum.CUSIP.getColumnName(), fields[1].getBytes());
		put.addColumn(COL_FAM, GnmaLoanColumnEnum.GNMA_ISSUER_NUM.getColumnName(), fields[2].getBytes());
		put.addColumn(COL_FAM, GnmaLoanColumnEnum.AGENCY.getColumnName(), fields[3].getBytes());
		put.addColumn(COL_FAM, GnmaLoanColumnEnum.LOAN_PURPOSE.getColumnName(), fields[4].getBytes());
		put.addColumn(COL_FAM, GnmaLoanColumnEnum.REFI_TYPE.getColumnName(), fields[5].getBytes());
		put.addColumn(COL_FAM, GnmaLoanColumnEnum.FIRST_PAYMENT_DATE.getColumnName(), fields[6].getBytes());
		put.addColumn(COL_FAM, GnmaLoanColumnEnum.MATURITY_DATE.getColumnName(), fields[7].getBytes());
		put.addColumn(COL_FAM, GnmaLoanColumnEnum.NOTE_RATE.getColumnName(), fields[8].getBytes());
		put.addColumn(COL_FAM, GnmaLoanColumnEnum.ORIG_LOAN_AMT.getColumnName(), fields[9].getBytes());
		put.addColumn(COL_FAM, GnmaLoanColumnEnum.ORIG_UPB.getColumnName(), fields[10].getBytes());
		put.addColumn(COL_FAM, GnmaLoanColumnEnum.CURRENT_UPB.getColumnName(), fields[11].getBytes());
		put.addColumn(COL_FAM, GnmaLoanColumnEnum.ORIG_LOAN_TERM.getColumnName(), fields[12].getBytes());
		put.addColumn(COL_FAM, GnmaLoanColumnEnum.LOAN_AGE.getColumnName(), fields[13].getBytes());
		put.addColumn(COL_FAM, GnmaLoanColumnEnum.REM_MONTHS_TO_MATURITY.getColumnName(), fields[14].getBytes());
		put.addColumn(COL_FAM, GnmaLoanColumnEnum.MONTHS_DELINQ.getColumnName(), fields[15].getBytes());
		put.addColumn(COL_FAM, GnmaLoanColumnEnum.MONTHS_PREPAID.getColumnName(), fields[16].getBytes());
		put.addColumn(COL_FAM, GnmaLoanColumnEnum.GROSS_MARGIN.getColumnName(), fields[17].getBytes());
		put.addColumn(COL_FAM, GnmaLoanColumnEnum.ORIG_LTV.getColumnName(), fields[18].getBytes());
		put.addColumn(COL_FAM, GnmaLoanColumnEnum.ORIG_CLTV.getColumnName(), fields[19].getBytes());
		put.addColumn(COL_FAM, GnmaLoanColumnEnum.ORIG_DTI.getColumnName(), fields[20].getBytes());
		put.addColumn(COL_FAM, GnmaLoanColumnEnum.CREDIT_SCORE.getColumnName(), fields[21].getBytes());
		put.addColumn(COL_FAM, GnmaLoanColumnEnum.DOWN_PAYM_ASSIST.getColumnName(), fields[22].getBytes());
		put.addColumn(COL_FAM, GnmaLoanColumnEnum.BUY_DOWN_STATUS.getColumnName(), fields[23].getBytes());
		put.addColumn(COL_FAM, GnmaLoanColumnEnum.UPFRONT_MIP.getColumnName(), fields[24].getBytes());
		put.addColumn(COL_FAM, GnmaLoanColumnEnum.ANNUAL_MIP.getColumnName(), fields[25].getBytes());
		put.addColumn(COL_FAM, GnmaLoanColumnEnum.NUM_BORROWERS.getColumnName(), fields[26].getBytes());
		put.addColumn(COL_FAM, GnmaLoanColumnEnum.FIRST_TIME_BUYER.getColumnName(), fields[27].getBytes());
		put.addColumn(COL_FAM, GnmaLoanColumnEnum.NUM_UNITS.getColumnName(), fields[28].getBytes());
		put.addColumn(COL_FAM, GnmaLoanColumnEnum.STATE.getColumnName(), fields[29].getBytes());
		put.addColumn(COL_FAM, GnmaLoanColumnEnum.MSA.getColumnName(), fields[30].getBytes());
		put.addColumn(COL_FAM, GnmaLoanColumnEnum.TPO_FLAG.getColumnName(), fields[31].getBytes());
		put.addColumn(COL_FAM, GnmaLoanColumnEnum.CURR_MONTH_LIQ_FLAG.getColumnName(), fields[32].getBytes());
		put.addColumn(COL_FAM, GnmaLoanColumnEnum.REMOVAL_REASON.getColumnName(), fields[33].getBytes());
		put.addColumn(COL_FAM, GnmaLoanColumnEnum.EFF_DATE.getColumnName(), fields[34].getBytes());
		put.addColumn(COL_FAM, GnmaLoanColumnEnum.LOAN_ORIG_DATE.getColumnName(), fields[35].getBytes());
		put.addColumn(COL_FAM, GnmaLoanColumnEnum.SELLER_ISSUER_ID.getColumnName(), fields[36].getBytes());
		put.addColumn(COL_FAM, GnmaLoanColumnEnum.AS_OF_DATE.getColumnName(), this.asOfDate.getBytes());
		String lastChgDateStr = lastChgDate + "";
		put.addColumn(COL_FAM, GnmaLoanColumnEnum.LAST_CHG_DATE.getColumnName(),lastChgDateStr.getBytes());

		return put;
	}

	public Put createArmLoanPut(String[] fields, long lastChgDate) {
		Put put=new Put(Bytes.toBytes(fields[0]));
		put.addColumn(COL_FAM, GnmaArmLoanColumnEnum.CUSIP.getColumnName(), fields[1].getBytes());
		put.addColumn(COL_FAM, GnmaArmLoanColumnEnum.EFF_DATE.getColumnName(), fields[34].getBytes());
		put.addColumn(COL_FAM, GnmaArmLoanColumnEnum.INDEX_TYPE.getColumnName(), fields[37].getBytes());
		put.addColumn(COL_FAM, GnmaArmLoanColumnEnum.LOOKBACK.getColumnName(), fields[38].getBytes());
		put.addColumn(COL_FAM, GnmaArmLoanColumnEnum.INTEREST_CHG_DATE.getColumnName(), fields[39].getBytes());
		put.addColumn(COL_FAM, GnmaArmLoanColumnEnum.INIT_RATE_CAP.getColumnName(), fields[40].getBytes());
		put.addColumn(COL_FAM, GnmaArmLoanColumnEnum.SUB_INIT_RATE_CAP.getColumnName(), fields[41].getBytes());
		put.addColumn(COL_FAM, GnmaArmLoanColumnEnum.LIFE_TIME_RATE_CAP.getColumnName(), fields[42].getBytes());
		put.addColumn(COL_FAM, GnmaArmLoanColumnEnum.NEXT_INTEREST_RATE_CEILING.getColumnName(), fields[43].getBytes());
		put.addColumn(COL_FAM, GnmaArmLoanColumnEnum.LIFE_TIME_RATE_CEILING.getColumnName(), fields[44].getBytes());
		put.addColumn(COL_FAM, GnmaArmLoanColumnEnum.LIFE_TIME_RATE_FLOOR.getColumnName(), fields[45].getBytes());
		put.addColumn(COL_FAM, GnmaArmLoanColumnEnum.PROSPECT_INTEREST_RATE.getColumnName(), fields[46].getBytes());
		put.addColumn(COL_FAM, GnmaArmLoanColumnEnum.AS_OF_DATE.getColumnName(), this.asOfDate.getBytes());
		
		String lastChgDateStr = lastChgDate + "";
		put.addColumn(COL_FAM, GnmaArmLoanColumnEnum.LAST_CHG_DATE.getColumnName(), lastChgDateStr.getBytes());

		return put;
	}

}
