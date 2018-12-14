package com.yieldbook.mortgage.hbase.action;

import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Calendar;

import org.apache.commons.lang3.StringUtils;

import com.opencsv.CSVParser;
import com.opencsv.CSVReader;
import com.opencsv.CSVWriter;
import com.opencsv.ICSVParser;

public class GnmaMonthlyFileParser extends BaseFileParser {

	final static int LOAN_LENGTH = 39;
	final static int ARM_LOAN_LENGTH = 15;

	String inputFileName;
	StringBuilder sb = new StringBuilder();
	CSVWriter loanPen = null;
	CSVWriter armLoanPen = null;
	String asOfDate;
	String lastChgDate;

	public GnmaMonthlyFileParser(String inputFileName, String asOfDateStr, String lastChgDateStr) {
		super();
		this.inputFileName = inputFileName;
		this.asOfDate = asOfDateStr;
		this.lastChgDate = lastChgDateStr;
		
		String loanFileName = inputFileName.substring(0,inputFileName.length()-4);
		loanFileName+="_loan.csv";
		String armLoanFileName = inputFileName.substring(0,inputFileName.length()-4);
		armLoanFileName+="_arm_loan.csv";
		System.out.println("loanFileName="+loanFileName);
		System.out.println("armLoanFileName="+armLoanFileName);

		try {
			loanPen = new CSVWriter(
					new FileWriter(loanFileName, true), '|',
					CSVWriter.NO_QUOTE_CHARACTER,
					CSVWriter.DEFAULT_ESCAPE_CHARACTER,
					CSVWriter.DEFAULT_LINE_END);
			armLoanPen = new CSVWriter(new FileWriter(
					armLoanFileName, true), '|',
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

					String[] loanArmFields = new String[ARM_LOAN_LENGTH];
					insertLoanFields(TableTypeEnum.ARM_LOAN, lastChgDate,
							line, loanArmFields);
					armLoanPen.writeNext(loanArmFields);
				}
			} // end of While
			if (loanPen != null)
				loanPen.close();
			if (armLoanPen != null)
				armLoanPen.close();

		} catch (IOException e) {
			System.out.println("Cannot parse line + \"" + line + "\"");
			e.printStackTrace();
		}

	}

}
