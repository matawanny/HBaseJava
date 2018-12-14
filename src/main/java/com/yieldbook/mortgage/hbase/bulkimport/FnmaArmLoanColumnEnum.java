package com.yieldbook.mortgage.hbase.bulkimport;

/**
 * HBase table columns for the 'srv' column family
 */
public enum FnmaArmLoanColumnEnum {
	LOAN_IDENTIFIER("loan_identifier".getBytes()),
	CUSIP("cusip".getBytes()),
	CONVERTIBLE_FLAG("convertible_flag".getBytes()),
	RATE_ADJMT_FREQ("rate_adjmt_freq".getBytes()),
	INITIAL_PERIOD("initial_period".getBytes()),
	NEXT_ADJMT_DATE("next_adjmt_date".getBytes()),
	LOOKBACK("lookback".getBytes()),
	GROSS_MARGIN("gross_margin".getBytes()),
	NET_MARGIN("net_margin".getBytes()),
	NET_MAX_LIFE_RATE("net_max_life_rate".getBytes()),
	MAX_LIFE_RATE("max_life_rate".getBytes()),
	INIT_CAP_UP("init_cap_up".getBytes()),
	INIT_CAP_DN("init_cap_dn".getBytes()),
	PERIODIC_CAP_UP("periodic_cap_up".getBytes()),
	PERIODIC_CAP_DN("periodic_cap_dn".getBytes()),
	MONTHS_TO_ADJUST("months_to_adjust".getBytes()),
	INDEX_NUM("index_num".getBytes()),
	LAST_CHG_DATE("last_chg_date".getBytes()),
	EFF_DATE("eff_date".getBytes()),
	AS_OF_DATE("as_of_date".getBytes());
	
	private final byte[] columnName;

	FnmaArmLoanColumnEnum(byte[] column) {
		this.columnName = column;
	}

	public byte[] getColumnName() {
		return this.columnName;
	}
}

