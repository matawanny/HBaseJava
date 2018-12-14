package com.yieldbook.mortgage.hbase.bulkimport;

/**
 * HBase table columns for the 'srv' column family
 */
public enum FnmaModLoanColumnEnum {
	LOAN_IDENTIFIER("loan_identifier".getBytes()),
	CUSIP("cusip".getBytes()),
	EFF_DATE("eff_date".getBytes()),
	LAST_CHG_DATE("last_chg_date".getBytes()),
	DAYS_DELINQUENT("days_delinquent".getBytes()),
	LOAN_PERFORMANCE_HISTORY("loan_performance_history".getBytes()),
	MOD_DATE_LOAN_AGE("mod_date_loan_age".getBytes()),
	MOD_PROGRAM("mod_program".getBytes()),
	MOD_TYPE("mod_type".getBytes()),
	NUM_OF_MODS("num_of_mods".getBytes()),
	TOT_CAPITALIZED_AMT("tot_capitalized_amt".getBytes()),
	ORIGIN_LOAN_AMT("origin_loan_amt".getBytes()),
	DEFERRED_UPB("deferred_upb".getBytes()),
	RATE_STEP_IND("rate_step_ind".getBytes()),
	INITIAL_FIXED_PER("initial_fixed_per".getBytes()),
	TOT_STEPS("tot_steps".getBytes()),
	REM_STEPS("rem_steps".getBytes()),
	NEXT_STEP_RATE("next_step_rate".getBytes()),
	TERMINAL_STEP_RATE("terminal_step_rate".getBytes()),
	TERMINAL_STEP_DATE("terminal_step_date".getBytes()),
	RATE_ADJ_FREQ("rate_adj_freq".getBytes()),
	MONTHS_TO_ADJ("months_to_adj".getBytes()),
	NEXT_ADJ_DATE("next_adj_date".getBytes()),
	PERIODIC_CAP_UP("periodic_cap_up".getBytes()),
	ORIGIN_CHANNEL("origin_channel".getBytes()),
	ORIGIN_NOTE_RATE("origin_note_rate".getBytes()),
	ORIGIN_UPB("origin_upb".getBytes()),
	ORIGIN_LOAN_TERM("origin_loan_term".getBytes()),
	ORIGIN_FIRST_PAYM_DATE("origin_first_paym_date".getBytes()),
	ORIGIN_MATURITY_DATE("origin_maturity_date".getBytes()),
	ORIGIN_LTV("origin_ltv".getBytes()),
	ORIGIN_CLTV("origin_cltv".getBytes()),
	ORIGIN_DTI("origin_dti".getBytes()),
	ORIGIN_CREDIT_SCORE("origin_credit_score".getBytes()),
	ORIGIN_LOAN_PURPOSE("origin_loan_purpose".getBytes()),
	ORIGIN_OCCUPANCY_STATUS("origin_occupancy_status".getBytes()),
	ORIGIN_PRODUCT_TYPE("origin_product_type".getBytes()),
	ORIGIN_IO_FLAG("origin_io_flag".getBytes()),
	AS_OF_DATE("as_of_date".getBytes());
	
	private final byte[] columnName;

	FnmaModLoanColumnEnum(byte[] column) {
		this.columnName = column;
	}

	public byte[] getColumnName() {
		return this.columnName;
	}
}

