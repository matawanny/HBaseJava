package com.yieldbook.mortgage.hbase.bulkimport;

/**
 * HBase table columns for the 'srv' column family
 */
public enum GnmaArmLoanColumnEnum {
	CUSIP("cusip".getBytes()),
	LOAN_SEQ_NUM("loan_seq_num".getBytes()),
	INDEX_TYPE("index_type".getBytes()),
	LOOKBACK("lookback".getBytes()),
	INTEREST_CHG_DATE("interest_chg_date".getBytes()),
	INIT_RATE_CAP("init_rate_cap".getBytes()),
	SUB_INIT_RATE_CAP("sub_init_rate_cap".getBytes()),
	LIFE_TIME_RATE_CAP("life_time_rate_cap".getBytes()),
	NEXT_INTEREST_RATE_CEILING("next_interest_rate_ceiling".getBytes()),
	LIFE_TIME_RATE_CEILING("life_time_rate_ceiling".getBytes()),
	LIFE_TIME_RATE_FLOOR("life_time_rate_floor".getBytes()),
	PROSPECT_INTEREST_RATE("prospect_interest_rate".getBytes()),
	LAST_CHG_DATE("lastChgDate".getBytes()),
	EFF_DATE("eff_date".getBytes()),
	AS_OF_DATE("as_of_date".getBytes());
	
	private final byte[] columnName;

	GnmaArmLoanColumnEnum(byte[] column) {
		this.columnName = column;
	}

	public byte[] getColumnName() {
		return this.columnName;
	}
}

