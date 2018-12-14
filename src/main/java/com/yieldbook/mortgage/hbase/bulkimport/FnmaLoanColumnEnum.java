package com.yieldbook.mortgage.hbase.bulkimport;

/**
 * HBase table columns for the 'srv' column family
 */
public enum FnmaLoanColumnEnum {
	LOAN_IDENTIFIER("loan_identifier".getBytes()),
	CUSIP("cusip".getBytes()),
	PROD_TYPE_IND("prod_type_ind".getBytes()),
	LOAN_PURPOSE("loan_purpose".getBytes()),
	OCCUPANCY_TYPE("occupancy_type".getBytes()),
	NUM_UNITS("num_units".getBytes()),
	STATE("state".getBytes()),
	CREDIT_SCORE("credit_score".getBytes()),
	ORIG_LOAN_TERM("orig_loan_term".getBytes()),
	ORIG_LTV("orig_ltv".getBytes()),
	PREPAY_PREMIUM_TERM("prepay_premium_term".getBytes()),
	IO_FLAG("io_flag".getBytes()),
	FIRST_PAYMENT_DATE("first_payment_date".getBytes()),
	FIRST_PI_DATE("first_pi_date".getBytes()),
	MATURITY_DATE("maturity_date".getBytes()),
	ORIG_NOTE_RATE("orig_note_rate".getBytes()),
	NOTE_RATE("note_rate".getBytes()),
	NET_NOTE_RATE("net_note_rate".getBytes()),
	ORIG_LOAN_SIZE("orig_loan_size".getBytes()),
	LOAN_AGE("loan_age".getBytes()),
	REM_MONTHS_TO_MATURITY("rem_months_to_maturity".getBytes()),
	MONTHS_TO_AMORTIZE("months_to_amortize".getBytes()),
	SERVICER("servicer".getBytes()),
	SELLER("seller".getBytes()),
	LAST_CHG_DATE("last_chg_date".getBytes()),
	CURRENT_UPB("current_upb".getBytes()),
	EFF_DATE("eff_date".getBytes()),
	ORIG_DTI("orig_dti".getBytes()),
	FIRST_TIME_BUYER("first_time_buyer".getBytes()),
	INS_PERCENT("ins_percent".getBytes()),
	NUM_BORROWERS("num_borrowers".getBytes()),
	ORIG_CLTV("orig_cltv".getBytes()),
	PROPERTY_TYPE("property_type".getBytes()),
	TPO_FLAG("tpo_flag".getBytes()),
	AS_OF_DATE("as_of_date".getBytes());	
	
	private final byte[] columnName;

	FnmaLoanColumnEnum(byte[] column) {
		this.columnName = column;
	}

	public byte[] getColumnName() {
		return this.columnName;
	}
}

