CREATE TABLE new.fhlmc_daily_hbase_table(rowkey string, x int, y int) 

STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'

WITH SERDEPROPERTIES ("hbase.columns.mapping" = ":key,cf:x,cf:y", 
"hbase.table.default.storage.type" = "binary");



CREATE TABLE fhlmc_hbase_daily (loan_identifier STRING,loan_correction_indicator STRING,
prefix STRING,security_identifier STRING,cusip STRING,mortgage_loan_amount DOUBLE,
issuance_investor_loan_upb DOUBLE,current_investor_loan_upb DOUBLE,amortization_type STRING,
original_interest_rate FLOAT,issuance_interest_rate FLOAT,current_interest_rate FLOAT,
issuance_net_interest_rate FLOAT,current_net_interest_rate FLOAT,first_payment_date BIGINT,
maturity_date BIGINT,loan_term BIGINT,remaining_months_to_maturity INT,loan_age INT,
ltv INT,cltv INT,dti INT,borrower_credit_score INT,f1 STRING,f2 STRING,f3 STRING,
number_of_borrowers INT,first_time_home_buyer_indicator STRING,loan_purpose STRING,
occupancy_status STRING,number_of_units INT,property_type STRING,channel STRING,property_state STRING,
seller_name STRING,servicer_name STRING,mortgage_insurance_percent INT,
mortgage_insurance_cancellation_indicator STRING,government_insured_guarantee STRING,
assumability_indicator STRING,interest_only_loan_indicator STRING,
interest_only_first_principal_and_interest_payment_date BIGINT,months_to_amortization INT,
prepayment_penalty_indicator STRING,prepayment_penalty_total_term STRING,index STRING,
mortgage_margin FLOAT, mbs_pc_margin FLOAT,interest_rate_adjustment_frequency INT,
interest_rate_lookback INT, interest_rate_rounding_method STRING,
interest_rate_rounding_method_percent STRING, convertibility_indicator STRING, 
initial_fixed_rate_period STRING, next_interest_rate_adjustment_date BIGINT, 
months_to_next_interest_rate_adjustment_date INT, life_ceiling_interest_rate FLOAT, 
life_ceiling_net_interest_rate FLOAT, life_floor_interest_rate FLOAT, 
life_floor_net_interest_rate FLOAT, initial_interest_rate_cap_up_percent FLOAT, 
initial_interest_rate_cap_down_percent FLOAT, periodic_interest_rate_cap_up_percent FLOAT, 
periodic_interest_rate_cap_down_percent FLOAT, modification_program STRING, 
modification_type STRING, number_of_modifications INT, total_capitalized_amount DOUBLE, 
interest_bearing_mortgage_loan_amount DOUBLE, original_deferred_amount DOUBLE, 
current_deferred_upb DOUBLE, loan_age_as_of_modification INT, eltv INT, 
updated_credit_score INT, f4 STRING, interest_rate_step_indicator STRING, 
initial_step_fixed_rate_period INT, total_number_of_steps BIGINT, number_of_remaining_steps INT, 
next_step_rate FLOAT, terminal_step_rate FLOAT, terminal_step_date BIGINT, 
step_rate_adjustment_frequency INT, next_step_rate_adjustment_date BIGINT, 
months_to_next_step_rate_adjustment_date INT, periodic_step_cap_up_percent FLOAT, 
origination_mortgage_loan_amount DOUBLE, origination_interest_rate FLOAT, 
origination_amortization_type STRING, origination_interest_only_loan_indicator STRING, 
origination_first_payment_date BIGINT, origination_maturity_date BIGINT, 
origination_loan_term INT, origination_ltv INT, origination_cltv INT, 
origination_debt_to_income_ratio INT, origination_credit_score INT, f5 STRING, 
f6 STRING, f7 STRING, origination_loan_purpose STRING, origination_occupancy_status STRING, 
origination_channel STRING, days_delinquent INT, loan_performance_history STRING, as_of_date BIGINT)
row format delimited fields terminated by '|' lines terminated by '\n'
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler' 
WITH SERDEPROPERTIES("hbase.table.default.storage.type" = "binary", "serialization.format"="1",
"hbase.columns.mapping"=":key,common:loan_correction_indicator,common:prefix,
common:security_identifier,common:cusip,loan:mortgage_loan_amount,loan:issuance_investor_loan_upb,
loan:current_investor_loan_upb,loan:amortization_type,loan:original_interest_rate,
loan:issuance_interest_rate,loan:current_interest_rate,loan:issuance_net_interest_rate,
loan:current_net_interest_rate,loan:first_payment_date,loan:maturity_date,loan:loan_term,
loan:remaining_months_to_maturity,loan:loan_age,loan:ltv,loan:cltv,loan:dti,
loan:borrower_credit_score,loan:f1,loan:f2,loan:f3,loan:number_of_borrowers,
loan:first_time_home_buyer_indicator,loan:loan_purpose,loan:occupancy_status,
loan:number_of_units,loan:property_type,loan:channel,loan:property_state,loan:seller_name,
loan:servicer_name,loan:mortgage_insurance_percent,loan:mortgage_insurance_cancellation_indicator,
loan:government_insured_guarantee,loan:assumability_indicator,loan:interest_only_loan_indicator,
loan:interest_only_first_principal_and_interest_payment_date,loan:months_to_amortization,
loan:prepayment_penalty_indicator,loan:prepayment_penalty_total_term,arm_loan:index,
arm_loan:mortgage_margin,arm_loan:mbs_pc_margin,arm_loan:interest_rate_adjustment_frequency,
arm_loan:interest_rate_lookback,arm_loan:interest_rate_rounding_method,
arm_loan:interest_rate_rounding_method_percent,arm_loan:convertibility_indicator,
arm_loan:initial_fixed_rate_period,arm_loan:next_interest_rate_adjustment_date,
arm_loan:months_to_next_interest_rate_adjustment_date,arm_loan:life_ceiling_interest_rate,
arm_loan:life_ceiling_net_interest_rate,arm_loan:life_floor_interest_rate,
arm_loan:life_floor_net_interest_rate,arm_loan:initial_interest_rate_cap_up_percent,
arm_loan:initial_interest_rate_cap_down_percent,arm_loan:periodic_interest_rate_cap_up_percent,
arm_loan:periodic_interest_rate_cap_down_percent,mod_loan:modification_program,
mod_loan:modification_type,mod_loan:number_of_modifications,mod_loan:total_capitalized_amount,
mod_loan:interest_bearing_mortgage_loan_amount,mod_loan:original_deferred_amount,
mod_loan:current_deferred_upb,mod_loan:loan_age_as_of_modification,mod_loan:eltv,
mod_loan:updated_credit_score,mod_loan:f4,mod_loan:interest_rate_step_indicator,
mod_loan:initial_step_fixed_rate_period,mod_loan:total_number_of_steps,
mod_loan:number_of_remaining_steps,mod_loan:next_step_rate,mod_loan:terminal_step_rate,
mod_loan:terminal_step_date,mod_loan:step_rate_adjustment_frequency,
mod_loan:next_step_rate_adjustment_date,mod_loan:months_to_next_step_rate_adjustment_date,
mod_loan:periodic_step_cap_up_percent,mod_loan:origination_mortgage_loan_amount,
mod_loan:origination_interest_rate,mod_loan:origination_amortization_type,
mod_loan:origination_interest_only_loan_indicator,mod_loan:origination_first_payment_date,
mod_loan:origination_maturity_date,mod_loan:origination_loan_term,mod_loan:origination_ltv,
mod_loan:origination_cltv,mod_loan:origination_debt_to_income_ratio,mod_loan:origination_credit_score,
mod_loan:f5,mod_loan:f6,mod_loan:f7,mod_loan:origination_loan_purpose,
mod_loan:origination_occupancy_status,mod_loan:origination_channel,mod_loan:days_delinquent,
mod_loan:loan_performance_history,common:as_of_date" )
TBLPROPERTIES("hbase.table.name" = "fhlmc_hbase_daily","serialization.null.format"="");



CREATE EXTERNAL TABLE hbase_userFace(id string, mobile string,name string) 
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'   
WITH SERDEPROPERTIES("hbase.columns.mapping" = ":key,faces:mobile,faces:name")  
TBLPROPERTIES("hbase.table.name" = "userFace");



CREATE EXTERNAL TABLE default.customers (   id BIGINT,   customeraddress STRING,   
customername STRING ) STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ('hbase.columns.mapping'=':key,common:customerName,common:customerAddress',

 'hbase.table.default.storage.type'='binary', 'serialization.format'='1') 
TBLPROPERTIES ('storage_handler'='org.apache.hadoop.hive.hbase.HBaseStorageHandler')






insert overwrite table fhlmc_hbase_daily
select loan_identifier,
loan_correction_indicator,prefix,security_identifier,cusip,mortgage_loan_amount,
issuance_investor_loan_upb,current_investor_loan_upb,amortization_type,original_interest_rate,
issuance_interest_rate,current_interest_rate,issuance_net_interest_rate,current_net_interest_rate,
first_payment_date,maturity_date,loan_term,remaining_months_to_maturity,loan_age,ltv,cltv,dti,
borrower_credit_score,f1,f2,f3,number_of_borrowers,first_time_home_buyer_indicator,loan_purpose,
occupancy_status,number_of_units,property_type,channel,property_state,seller_name,servicer_name,
mortgage_insurance_percent,mortgage_insurance_cancellation_indicator,government_insured_guarantee,
assumability_indicator,interest_only_loan_indicator,
interest_only_first_principal_and_interest_payment_date,months_to_amortization,
prepayment_penalty_indicator,prepayment_penalty_total_term,index,mortgage_margin,mbs_pc_margin,
interest_rate_adjustment_frequency,interest_rate_lookback,interest_rate_rounding_method,
interest_rate_rounding_method_percent,convertibility_indicator,initial_fixed_rate_period,
next_interest_rate_adjustment_date,months_to_next_interest_rate_adjustment_date,
life_ceiling_interest_rate,life_ceiling_net_interest_rate,life_floor_interest_rate,
life_floor_net_interest_rate,initial_interest_rate_cap_up_percent,initial_interest_rate_cap_down_percent,
periodic_interest_rate_cap_up_percent,periodic_interest_rate_cap_down_percent,modification_program,
modification_type,number_of_modifications,total_capitalized_amount,interest_bearing_mortgage_loan_amount,
original_deferred_amount,current_deferred_upb,loan_age_as_of_modification,eltv,updated_credit_score,
f4,interest_rate_step_indicator,initial_step_fixed_rate_period,total_number_of_steps,
number_of_remaining_steps,next_step_rate,terminal_step_rate,terminal_step_date,
step_rate_adjustment_frequency,next_step_rate_adjustment_date,months_to_next_step_rate_adjustment_date,
periodic_step_cap_up_percent,origination_mortgage_loan_amount,origination_interest_rate,
origination_amortization_type,origination_interest_only_loan_indicator,origination_first_payment_date,
origination_maturity_date,origination_loan_term,origination_ltv,origination_cltv,
origination_debt_to_income_ratio,origination_credit_score,f5,f6,f7,origination_loan_purpose,
origination_occupancy_status,origination_channel,days_delinquent,loan_performance_history,
as_of_date
from prd.fhlmonly where year=2018 and month=5 and amortization_type='ARM'



create temporary function row_sequence as 

'org.apache.hadoop.hive.contrib.udf.UDFRowSequence';

select loan_identifier from

(select loan_identifier

from  prd.fhlmonly

tablesample(bucket 1 out of 10000 on transaction_id) s 

order by transaction_id 

limit 10000000) x

where (row_sequence() % 910000)=0

order by transaction_id

limit 11;





CREATE EXTERNAL TABLE fhlmc_hbase_daily_1 (loan_identifier STRING,loan_correction_indicator STRING,
prefix STRING,security_identifier STRING,cusip STRING,mortgage_loan_amount DOUBLE,
issuance_investor_loan_upb DOUBLE,current_investor_loan_upb DOUBLE,amortization_type STRING,
original_interest_rate FLOAT,issuance_interest_rate FLOAT,current_interest_rate FLOAT,
issuance_net_interest_rate FLOAT,current_net_interest_rate FLOAT,first_payment_date BIGINT,
maturity_date BIGINT,loan_term BIGINT,remaining_months_to_maturity INT,loan_age INT,
ltv INT,cltv INT,dti INT,borrower_credit_score INT,f1 STRING,f2 STRING,f3 STRING,
number_of_borrowers INT,first_time_home_buyer_indicator STRING,loan_purpose STRING,
occupancy_status STRING,number_of_units INT,property_type STRING,channel STRING,property_state STRING,
seller_name STRING,servicer_name STRING,mortgage_insurance_percent INT,
mortgage_insurance_cancellation_indicator STRING,government_insured_guarantee STRING,
assumability_indicator STRING,interest_only_loan_indicator STRING,
interest_only_first_principal_and_interest_payment_date BIGINT,months_to_amortization INT,
prepayment_penalty_indicator STRING,prepayment_penalty_total_term STRING,index STRING,
mortgage_margin FLOAT, mbs_pc_margin FLOAT,interest_rate_adjustment_frequency INT,
interest_rate_lookback INT, interest_rate_rounding_method STRING,
interest_rate_rounding_method_percent STRING, convertibility_indicator STRING, 
initial_fixed_rate_period STRING, next_interest_rate_adjustment_date BIGINT, 
months_to_next_interest_rate_adjustment_date INT, life_ceiling_interest_rate FLOAT, 
life_ceiling_net_interest_rate FLOAT, life_floor_interest_rate FLOAT, 
life_floor_net_interest_rate FLOAT, initial_interest_rate_cap_up_percent FLOAT, 
initial_interest_rate_cap_down_percent FLOAT, periodic_interest_rate_cap_up_percent FLOAT, 
periodic_interest_rate_cap_down_percent FLOAT, modification_program STRING, 
modification_type STRING, number_of_modifications INT, total_capitalized_amount DOUBLE, 
interest_bearing_mortgage_loan_amount DOUBLE, original_deferred_amount DOUBLE, 
current_deferred_upb DOUBLE, loan_age_as_of_modification INT, eltv INT, 
updated_credit_score INT, f4 STRING, interest_rate_step_indicator STRING, 
initial_step_fixed_rate_period INT, total_number_of_steps BIGINT, number_of_remaining_steps INT, 
next_step_rate FLOAT, terminal_step_rate FLOAT, terminal_step_date BIGINT, 
step_rate_adjustment_frequency INT, next_step_rate_adjustment_date BIGINT, 
months_to_next_step_rate_adjustment_date INT, periodic_step_cap_up_percent FLOAT, 
origination_mortgage_loan_amount DOUBLE, origination_interest_rate FLOAT, 
origination_amortization_type STRING, origination_interest_only_loan_indicator STRING, 
origination_first_payment_date BIGINT, origination_maturity_date BIGINT, 
origination_loan_term INT, origination_ltv INT, origination_cltv INT, 
origination_debt_to_income_ratio INT, origination_credit_score INT, f5 STRING, 
f6 STRING, f7 STRING, origination_loan_purpose STRING, origination_occupancy_status STRING, 
origination_channel STRING, days_delinquent INT, loan_performance_history STRING, as_of_date BIGINT)
row format delimited fields terminated by '|' lines terminated by '\n'
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler' 
WITH SERDEPROPERTIES("hbase.table.default.storage.type" = "binary", "serialization.format"="1",
"hbase.columns.mapping"=":key,common:loan_correction_indicator,common:prefix,
common:security_identifier,common:cusip,loan:mortgage_loan_amount,loan:issuance_investor_loan_upb,
loan:current_investor_loan_upb,loan:amortization_type,loan:original_interest_rate,
loan:issuance_interest_rate,loan:current_interest_rate,loan:issuance_net_interest_rate,
loan:current_net_interest_rate,loan:first_payment_date,loan:maturity_date,loan:loan_term,
loan:remaining_months_to_maturity,loan:loan_age,loan:ltv,loan:cltv,loan:dti,
loan:borrower_credit_score,loan:f1,loan:f2,loan:f3,loan:number_of_borrowers,
loan:first_time_home_buyer_indicator,loan:loan_purpose,loan:occupancy_status,
loan:number_of_units,loan:property_type,loan:channel,loan:property_state,loan:seller_name,
loan:servicer_name,loan:mortgage_insurance_percent,loan:mortgage_insurance_cancellation_indicator,
loan:government_insured_guarantee,loan:assumability_indicator,loan:interest_only_loan_indicator,
loan:interest_only_first_principal_and_interest_payment_date,loan:months_to_amortization,
loan:prepayment_penalty_indicator,loan:prepayment_penalty_total_term,arm_loan:index,
arm_loan:mortgage_margin,arm_loan:mbs_pc_margin,arm_loan:interest_rate_adjustment_frequency,
arm_loan:interest_rate_lookback,arm_loan:interest_rate_rounding_method,
arm_loan:interest_rate_rounding_method_percent,arm_loan:convertibility_indicator,
arm_loan:initial_fixed_rate_period,arm_loan:next_interest_rate_adjustment_date,
arm_loan:months_to_next_interest_rate_adjustment_date,arm_loan:life_ceiling_interest_rate,
arm_loan:life_ceiling_net_interest_rate,arm_loan:life_floor_interest_rate,
arm_loan:life_floor_net_interest_rate,arm_loan:initial_interest_rate_cap_up_percent,
arm_loan:initial_interest_rate_cap_down_percent,arm_loan:periodic_interest_rate_cap_up_percent,
arm_loan:periodic_interest_rate_cap_down_percent,mod_loan:modification_program,
mod_loan:modification_type,mod_loan:number_of_modifications,mod_loan:total_capitalized_amount,
mod_loan:interest_bearing_mortgage_loan_amount,mod_loan:original_deferred_amount,
mod_loan:current_deferred_upb,mod_loan:loan_age_as_of_modification,mod_loan:eltv,
mod_loan:updated_credit_score,mod_loan:f4,mod_loan:interest_rate_step_indicator,
mod_loan:initial_step_fixed_rate_period,mod_loan:total_number_of_steps,
mod_loan:number_of_remaining_steps,mod_loan:next_step_rate,mod_loan:terminal_step_rate,
mod_loan:terminal_step_date,mod_loan:step_rate_adjustment_frequency,
mod_loan:next_step_rate_adjustment_date,mod_loan:months_to_next_step_rate_adjustment_date,
mod_loan:periodic_step_cap_up_percent,mod_loan:origination_mortgage_loan_amount,
mod_loan:origination_interest_rate,mod_loan:origination_amortization_type,
mod_loan:origination_interest_only_loan_indicator,mod_loan:origination_first_payment_date,
mod_loan:origination_maturity_date,mod_loan:origination_loan_term,mod_loan:origination_ltv,
mod_loan:origination_cltv,mod_loan:origination_debt_to_income_ratio,mod_loan:origination_credit_score,
mod_loan:f5,mod_loan:f6,mod_loan:f7,mod_loan:origination_loan_purpose,
mod_loan:origination_occupancy_status,mod_loan:origination_channel,mod_loan:days_delinquent,
mod_loan:loan_performance_history,common:as_of_date" )
TBLPROPERTIES("hbase.table.name" = "fhlmc_hbase_daily");