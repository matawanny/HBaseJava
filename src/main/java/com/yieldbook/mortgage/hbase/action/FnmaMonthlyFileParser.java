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

public class FnmaMonthlyFileParser extends BaseFileParser {

	final static int LOAN_LENGTH = 35;
	final static int ARM_LOAN_LENGTH = 20;
	final static int MOD_LOAN_LENGTH = 39;

	String inputFileName;
	StringBuilder sb = new StringBuilder();
	CSVWriter loanPen = null;
	CSVWriter armLoanPen = null;
	CSVWriter modLoanPen = null;
	String afOfDateMonthly;
	String effDateMonthly;

	public FnmaMonthlyFileParser(String inputFileName, String asOfDateStr) {
		super();
		this.inputFileName = inputFileName;
		this.afOfDateMonthly = asOfDateStr;
		this.effDateMonthly= asOfDateStr.substring(0,6).concat("01");

		try {
			loanPen = new CSVWriter(
					new FileWriter("fnma_loan_monthly.csv", true), '|',
					CSVWriter.NO_QUOTE_CHARACTER,
					CSVWriter.DEFAULT_ESCAPE_CHARACTER,
					CSVWriter.DEFAULT_LINE_END);
			armLoanPen = new CSVWriter(new FileWriter(
					"fnma_arm_loan_monthly.csv", true), '|',
					CSVWriter.NO_QUOTE_CHARACTER,
					CSVWriter.DEFAULT_ESCAPE_CHARACTER,
					CSVWriter.DEFAULT_LINE_END);
			modLoanPen = new CSVWriter(new FileWriter(
					"fnma_mod_loan_monthly.csv", true), '|',
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
			des[26] = this.effDateMonthly; // eff_date
			des[27] = src[23]; // orig_dti
			des[28] = src[25]; // first_time_buyer
			des[29] = src[31]; // ins_percent
			des[30] = src[22]; // num_borrowers
			des[31] = src[21]; // orig_cltv
			des[32] = src[27]; // property_type
			des[33] = src[7]; // tpo_flag
			des[34] = this.afOfDateMonthly; // as_of_date_monthly
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
			des[18] = this.effDateMonthly; // eff_date
			des[19] = this.afOfDateMonthly; // as_of_date
			break;
		case MOD_LOAN:
			des[0] = src[6]; // loan_identifier
			des[1] = src[0]; // cusip
			des[2] = this.effDateMonthly; // eff_date
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
			des[38] = this.afOfDateMonthly; // as_of_date
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
				if (StringUtils.isEmpty(line[0])||StringUtils.isEmpty(line[6]))
					continue;
		
				String[] loanFields = new String[LOAN_LENGTH];
				insertLoanFields(TableTypeEnum.LOAN, lastChgDate, line,
						loanFields);
				loanPen.writeNext(loanFields);

				if (!StringUtils.isEmpty(line[32])
						&& line[32].equalsIgnoreCase("ARM")) {
					// row[32] mapping to Amortization_Type
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
					// line[52] mapping to days_delinquent
					// line[53] mapping to loan_performance_history
					// line[54] mapping to mod_date_loan_age
					// line[55] mapping to mod_program
					// line[56] mapping to mod_type
					// line[57] mapping to num_of_mods
					String[] loanModFields = new String[MOD_LOAN_LENGTH];
					insertLoanFields(TableTypeEnum.MOD_LOAN, lastChgDate,
							line, loanModFields);
					modLoanPen.writeNext(loanModFields);
				}

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

	}

}
