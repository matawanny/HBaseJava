package com.yieldbook.mortgage.hbase.bulkimport;

/**
 * HBase table columns for the 'srv' column family
 */
public enum FhlmcLoanColumnEnum {
	LOAN_IDENTIFIER("loan_identifier".getBytes()),
	CUSIP("cusip".getBytes()),
	PROD_TYPE_IND("prod_type_ind".getBytes()),
	LOAN_PURPOSE("loan_purpose".getBytes()),
	TPO_FLAG("tpo_flag".getBytes()),
	PROPERTY_TYPE("property_type".getBytes()),
	OCCUPANCY_STATUS("occupancy_status".getBytes()),
	NUM_UNITS("num_units".getBytes()),
	STATE("state".getBytes()),
	CREDIT_SCORE("credit_score".getBytes()),
	ORIG_LOAN_TERM("orig_loan_term".getBytes()),
	ORIG_LTV("orig_ltv".getBytes()),
	PREPAY_PENALTY_FLAG("prepay_penalty_flag".getBytes()),
	IO_FLAG("io_flag".getBytes()),
	FIRST_PAYMENT_DATE("first_payment_date".getBytes()),
	FIRST_PI_DATE("first_pi_date".getBytes()),
	MATURITY_DATE("maturity_date".getBytes()),
	ORIG_NOTE_RATE("orig_note_rate".getBytes()),
	PC_ISSUANCE_NOTE_RATE("pc_issuance_note_rate".getBytes()),
	NET_NOTE_RATE("net_note_rate".getBytes()),
	ORIG_LOAN_AMT("orig_loan_amt".getBytes()),
	ORIG_UPB("orig_upb".getBytes()),
	LOAN_AGE("loan_age".getBytes()),
	REM_MONTHS_TO_MATURITY("rem_months_to_maturity".getBytes()),
	MONTHS_TO_AMORTIZE("months_to_amortize".getBytes()),
	SERVICER("servicer".getBytes()),
	SELLER("seller".getBytes()),
	LAST_CHG_DATE("last_chg_date".getBytes()),
	CURRENT_UPB("current_upb".getBytes()),
	EFF_DATE("eff_date".getBytes()),
	DOC_ASSETS("doc_assets".getBytes()),
	DOC_EMPL("doc_empl".getBytes()),
	DOC_INCOME("doc_income".getBytes()),
	ORIG_CLTV("orig_cltv".getBytes()),
	NUM_BORROWERS("num_borrowers".getBytes()),
	FIRST_TIME_BUYER("first_time_buyer".getBytes()),
	INS_PERCENT("ins_percent".getBytes()),
	ORIG_DTI("orig_dti".getBytes()),
	MSA("msa".getBytes()),
	UPD_CREDIT_SCORE("upd_credit_score".getBytes()),
	ESTM_LTV("estm_ltv".getBytes()),
	CORRECTION_FLAG("correction_flag".getBytes()),
	PREFIX("prefix".getBytes()),
	INS_CANCEL_IND("ins_cancel_ind".getBytes()),
	GOVT_INS_GRNTE("govt_ins_grnte".getBytes()),
	ASSUMABILITY_IND("assumability_ind".getBytes()),
	PREPAY_TERM("prepay_term".getBytes()),
	AS_OF_DATE("as_of_date".getBytes());
	
	private final byte[] columnName;

	FhlmcLoanColumnEnum(byte[] column) {
		this.columnName = column;
	}

	public byte[] getColumnName() {
		return this.columnName;
	}
}

