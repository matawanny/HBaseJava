package com.yieldbook.mortgage.hbase.bulkimport;

/**
 * HBase table columns for the 'srv' column family
 */
public enum GnmaLoanColumnEnum {
	CUSIP("cusip".getBytes()),
	EFF_DATE("eff_date".getBytes()),
	LOAN_SEQ_NUM("loan_seq_num".getBytes()),
	GNMA_ISSUER_NUM("gnma_issuer_num".getBytes()),
	AGENCY("agency".getBytes()),
	LOAN_PURPOSE("loan_purpose".getBytes()),
	REFI_TYPE("refi_type".getBytes()),
	FIRST_PAYMENT_DATE("first_payment_date".getBytes()),
	MATURITY_DATE("maturity_date".getBytes()),
	NOTE_RATE("note_rate".getBytes()),
	ORIG_LOAN_AMT("orig_loan_amt".getBytes()),
	ORIG_UPB("orig_upb".getBytes()),
	CURRENT_UPB("current_upb".getBytes()),
	ORIG_LOAN_TERM("orig_loan_term".getBytes()),
	LOAN_AGE("loan_age".getBytes()),
	REM_MONTHS_TO_MATURITY("rem_months_to_maturity".getBytes()),
	MONTHS_DELINQ("months_delinq".getBytes()),
	MONTHS_PREPAID("months_prepaid".getBytes()),
	GROSS_MARGIN("gross_margin".getBytes()),
	ORIG_LTV("orig_ltv".getBytes()),
	ORIG_CLTV("orig_cltv".getBytes()),
	ORIG_DTI("orig_dti".getBytes()),
	CREDIT_SCORE("credit_score".getBytes()),
	DOWN_PAYM_ASSIST("down_paym_assist".getBytes()),
	BUY_DOWN_STATUS("buy_down_status".getBytes()),
	UPFRONT_MIP("upfront_mip".getBytes()),
	ANNUAL_MIP("annual_mip".getBytes()),
	NUM_BORROWERS("num_borrowers".getBytes()),
	FIRST_TIME_BUYER("first_time_buyer".getBytes()),
	NUM_UNITS("num_units".getBytes()),
	STATE("state".getBytes()),
	MSA("msa".getBytes()),
	TPO_FLAG("tpo_flag".getBytes()),
	CURR_MONTH_LIQ_FLAG("curr_month_liq_flag".getBytes()),
	REMOVAL_REASON("removal_reason".getBytes()),
	LAST_CHG_DATE("lastChgDate".getBytes()),
	LOAN_ORIG_DATE("loan_orig_date".getBytes()),
	SELLER_ISSUER_ID("seller_issuer_id".getBytes()),
	AS_OF_DATE("as_of_date".getBytes());	
	
	private final byte[] columnName;

	GnmaLoanColumnEnum(byte[] column) {
		this.columnName = column;
	}

	public byte[] getColumnName() {
		return this.columnName;
	}
}

