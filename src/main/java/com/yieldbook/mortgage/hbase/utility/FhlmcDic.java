package com.yieldbook.mortgage.hbase.utility;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

public class FhlmcDic{
	
	public static final String FHLMC_HBASE_TABLE = "fhlmc_hbase_daily";
	public static final String[] Columns = new String[] { "loan_identifier",
		"loan_correction_indicator", "prefix", "security_identifier",
		"cusip", "mortgage_loan_amount", "issuance_investor_loan_upb",
		"current_investor_loan_upb", "amortization_type",
		"original_interest_rate", "issuance_interest_rate",
		"current_interest_rate", "issuance_net_interest_rate",
		"current_net_interest_rate", "first_payment_date", "maturity_date",
		"loan_term", "remaining_months_to_maturity", "loan_age", "ltv",
		"cltv", "dti", "borrower_credit_score", "f1", "f2", "f3",
		"number_of_borrowers", "first_time_home_buyer_indicator",
		"loan_purpose", "occupancy_status", "number_of_units",
		"property_type", "channel", "property_state", "seller_name",
		"servicer_name", "mortgage_insurance_percent",
		"mortgage_insurance_cancellation_indicator",
		"government_insured_guarantee", "assumability_indicator",
		"interest_only_loan_indicator",
		"interest_only_first_principal_and_interest_payment_date",
		"months_to_amortization", "prepayment_penalty_indicator",
		"prepayment_penalty_total_term", "index", "mortgage_margin",
		"mbs_pc_margin", "interest_rate_adjustment_frequency",
		"interest_rate_lookback", "interest_rate_rounding_method",
		"interest_rate_rounding_method_percent",
		"convertibility_indicator", "initial_fixed_rate_period",
		"next_interest_rate_adjustment_date",
		"months_to_next_interest_rate_adjustment_date",
		"life_ceiling_interest_rate", "life_ceiling_net_interest_rate",
		"life_floor_interest_rate", "life_floor_net_interest_rate",
		"initial_interest_rate_cap_up_percent",
		"initial_interest_rate_cap_down_percent",
		"periodic_interest_rate_cap_up_percent",
		"periodic_interest_rate_cap_down_percent", "modification_program",
		"modification_type", "number_of_modifications",
		"total_capitalized_amount",
		"interest_bearing_mortgage_loan_amount",
		"original_deferred_amount", "current_deferred_upb",
		"loan_age_as_of_modification", "eltv", "updated_credit_score",
		"f4", "interest_rate_step_indicator",
		"initial_step_fixed_rate_period", "total_number_of_steps",
		"number_of_remaining_steps", "next_step_rate",
		"terminal_step_rate", "terminal_step_date",
		"step_rate_adjustment_frequency", "next_step_rate_adjustment_date",
		"months_to_next_step_rate_adjustment_date",
		"periodic_step_cap_up_percent", "origination_mortgage_loan_amount",
		"origination_interest_rate", "origination_amortization_type",
		"origination_interest_only_loan_indicator",
		"origination_first_payment_date", "origination_maturity_date",
		"origination_loan_term", "origination_ltv", "origination_cltv",
		"origination_debt_to_income_ratio", "origination_credit_score",
		"f5", "f6", "f7", "origination_loan_purpose",
		"origination_occupancy_status", "origination_channel",
		"days_delinquent", "loan_performance_history", "as_of_date" };

	 public static final Map<Integer, String> ColumnFamilyMap = new HashMap<>();
	 
	 static{
		 ColumnFamilyMap.put(1,"common");
		 ColumnFamilyMap.put(2,"common");
		 ColumnFamilyMap.put(3,"common");
		 ColumnFamilyMap.put(4,"common");
		 ColumnFamilyMap.put(105,"common");
  
		 for(int i=5;i<=44;i++){
			 ColumnFamilyMap.put(i,"loan");
		 }
		 
		 for(int i=45;i<=63;i++){
			 ColumnFamilyMap.put(i,"arm-loan");
		 }

		 for(int i=94;i<=104;i++){
			 ColumnFamilyMap.put(i,"mod-loan");
		 }		 
	 }
	 
	 public static Delete createDelete(String loanIdentifier){
		 Delete delete = new Delete(Bytes.toBytes(loanIdentifier));
		 delete.addColumn(Bytes.toBytes("common"), Bytes.toBytes(Columns[1]));
		 delete.addColumn(Bytes.toBytes("common"), Bytes.toBytes(Columns[2]));
		 delete.addColumn(Bytes.toBytes("common"), Bytes.toBytes(Columns[3]));
		 delete.addColumn(Bytes.toBytes("common"), Bytes.toBytes(Columns[4]));
		 delete.addColumn(Bytes.toBytes("common"), Bytes.toBytes(Columns[105]));
		 
		 for(int i=5;i<=44;i++){
			 delete.addColumn(Bytes.toBytes("loan"), Bytes.toBytes(Columns[i]));
		 }
		 
		 for(int i=45;i<=63;i++){
			 delete.addColumn(Bytes.toBytes("arm-loan"), Bytes.toBytes(Columns[i]));
		 }

		 for(int i=94;i<=104;i++){
			 delete.addColumn(Bytes.toBytes("mod-loan"), Bytes.toBytes(Columns[i]));			 
		 }			 
		 return delete;
	 }
	 
	 public static Put createPut(String[] row){
		 Put put = new Put(Bytes.toBytes(row[0]));
		 put.addColumn(Bytes.toBytes("common"), Bytes.toBytes(Columns[1]), Bytes.toBytes(row[1]));
		 put.addColumn(Bytes.toBytes("common"), Bytes.toBytes(Columns[2]), Bytes.toBytes(row[2]));
		 put.addColumn(Bytes.toBytes("common"), Bytes.toBytes(Columns[3]), Bytes.toBytes(row[3]));
		 put.addColumn(Bytes.toBytes("common"), Bytes.toBytes(Columns[4]), Bytes.toBytes(row[4]));
		 put.addColumn(Bytes.toBytes("common"), Bytes.toBytes(Columns[105]), Bytes.toBytes(row[105]));
		 
		 for(int i=5;i<=44;i++){
			 put.addColumn(Bytes.toBytes("loan"), Bytes.toBytes(Columns[i]), Bytes.toBytes(row[i]));
		 }
		 
		 for(int i=45;i<=63;i++){
			 put.addColumn(Bytes.toBytes("arm_loan"), Bytes.toBytes(Columns[i]), Bytes.toBytes(row[i]));
		 }

		 for(int i=94;i<=104;i++){
			 put.addColumn(Bytes.toBytes("mod_loan"), Bytes.toBytes(Columns[i]), Bytes.toBytes(row[i]));			 
		 }			 
		 return put;
	 }	 
}
