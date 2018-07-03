#!/bin/bash

currentUser=$(whoami)
if [ "$currentUser" != "oozie" ]
then
    echo "Must login in as oozie"
    exit;
fi

AS_OF_DATE=$1
if [ -z "$AS_OF_DATE" ]
then
      echo "Must input as of date!"

      exit;
fi

ls $AS_OF_DATE
if [[ $? -ne 0 ]]
then
echo "Create folder $AS_OF_DATE"
mkdir $AS_OF_DATE
fi

cd $AS_OF_DATE

rm -rf *.dat

output_file=fhlmc_loan_monthly.dat

impala-shell -B -i ybgdev79.ny.ssmb.com -q "invalidate metadata; select cusip, cast(substr(loan_identifier, 8) as int) as loan_seq_num, 
CASE prod_type_ind WHEN 'FRM' THEN 'F' WHEN 'ARM' THEN 'A' ELSE NULL END as prod_type_ind, loan_purpose, tpo_flag, property_type, 
occupancy_status, num_units, state, credit_score, orig_loan_term, orig_ltv, 
NVL(prepay_penalty_flag, '') as prepay_penalty_flag, NVL(io_flag, '') as io_flag, 
NVL(from_unixtime(first_payment_date div 1000,'yyyyMMdd'), '') as first_payment_date, 
NVL(from_unixtime(first_pi_date div 1000,'yyyyMMdd'),'') as first_pi_date, 
NVL(from_unixtime(maturity_date div 1000,'yyyyMMdd'), '') as maturity_date, orig_note_rate, 
pc_issuance_note_rate, net_note_rate, orig_loan_amt, orig_upb, loan_age, rem_months_to_maturity, 
months_to_amortize, NVL(servicer, '') as servicer, NVL(seller, '') as seller, '' as last_chg_date, current_upb, 
from_unixtime(eff_date,'yyyyMMdd') as eff_date, NVL(doc_assets, '') as doc_assets, NVL(doc_empl, '') as doc_empl, 
NVL(doc_income, '') as doc_income, orig_cltv, NVL(num_borrowers, '') as num_borrowers, NVL(first_time_buyer, '') as first_time_buyer, 
ins_percent, orig_dti, NVL(msa, '') as msa, upd_credit_score, estm_ltv, NVL(correction_flag, '') as correction_flag, 
NVL(prefix, '') as prefix, NVL(ins_cancel_ind, '') as ins_cancel_ind, NVL(govt_ins_grnte, '') as govt_ins_grnte, 
NVL(assumability_ind, '') as assumability_ind, NVL(prepay_term, '') as prepay_term  from fhlmc_loan_monthly 
where as_of_date=$AS_OF_DATE " -o $output_file --print_header --output_delimiter=\|

firstWord=$(head -n  1 $output_file|cut -f 1 -d '|')
if [ "$firstWord" != "cusip" ]
then
   sed -i 1d $output_file
fi

FILELINES=$(wc -l $output_file | awk  '{print $1;}') 
if [ $FILELINES -eq 1 ]
then
   rm -rf $output_file
else 
	sed -i "s/NULL//g" $output_file
fi

output_file=fhlmc_arm_loan_monthly.dat

impala-shell -B -i ybgdev79.ny.ssmb.com -q "invalidate metadata; select cusip,cast(substr(loan_identifier, 8) as int) as loan_seq_num,
NVL(convertible_flag, '') as convertible_flag, rate_adjmt_freq, initial_period, 
NVL(from_unixtime(next_adjmt_date div 1000,'yyyyMMdd'),'') as next_adjmt_date, lookback, round(gross_margin,3) as gross_margin, round(net_margin,3) as net_margin, 
round(net_max_life_rate,3) as net_max_life_rate, round(max_life_rate,3) as max_life_rate, 
init_cap_up, init_cap_dn, periodic_cap, months_to_adjust, NVL(index_desc, '') as index_desc, '' as last_chg_date, 
from_unixtime(eff_date,'yyyyMMdd') as eff_date from fhlmc_arm_loan_monthly 
where as_of_date=$AS_OF_DATE " -o $output_file --print_header --output_delimiter=\|

firstWord=$(head -n  1 $output_file|cut -f 1 -d '|')
if [ "$firstWord" != "cusip" ]
then
   sed -i 1d $output_file
fi

FILELINES=$(wc -l $output_file | awk  '{print $1;}') 
if [ $FILELINES -eq 1 ]
then 
   rm -rf $output_file
else
	sed -i "s/NULL//g" $output_file
fi

output_file=fhlmc_mod_loan_monthly.dat

impala-shell -B -i ybgdev79.ny.ssmb.com -q "invalidate metadata; select cusip, cast(substr(loan_identifier, 8) as int) as loan_seq_num,  
from_unixtime(eff_date,'yyyyMMdd') as eff_date, NVL(correction_flag, '') as correction_flag, 
CASE product_type WHEN 'FRM' THEN 'F' WHEN 'ARM' THEN 'A' ELSE NULL END as product_type, 
NVL(origin_loan_purpose, '') as origin_loan_purpose, NVL(origin_tpo_flag, '') as origin_tpo_flag, NVL(origin_occupancy_status, '') as origin_occupancy_status, 
origin_credit_score, origin_loan_term, origin_ltv, NVL(origin_io_flag, '') as origin_io_flag, 
NVL(from_unixtime(origin_first_paym_date div 1000,'yyyyMMdd'),'') as origin_first_paym_date, 
NVL(from_unixtime(origin_maturity_date div 1000,'yyyyMMdd'),'') as origin_maturity_date, origin_note_rate, origin_loan_amt, 
origin_cltv, origin_dti, NVL(origin_product_type, '') as origin_product_type, mod_date_loan_age, NVL(mod_program, '') as mod_program, NVL(mod_type, '') as mod_type, num_of_mods, 
tot_capitalized_amt, '' as last_chg_date, int_bear_loan_amt, deferred_amt, deferred_upb, NVL(rate_step_ind, '') as rate_step_ind, tot_steps, 
rem_steps, initial_fixed_per, rate_adj_freq, periodic_cap_up, months_to_adj, next_step_rate, 
NVL(from_unixtime(next_adj_date div 1000,'yyyyMMdd'),'') as next_adj_date, terminal_step_rate, 
NVL(from_unixtime(terminal_step_date div 1000,'yyyyMMdd'),'') as terminal_step_date, cur_gross_note_rate  
from fhlmc_mod_loan_monthly where as_of_date=$AS_OF_DATE " -o $output_file --print_header --output_delimiter=\|

firstWord=$(head -n  1 $output_file|cut -f 1 -d '|')
if [ "$firstWord" != "cusip" ]
then
   sed -i 1d $output_file
fi

FILELINES=$(wc -l $output_file | awk  '{print $1;}') 
if [ $FILELINES -eq 1 ]
then
   rm -rf $output_file
else 
	sed -i "s/NULL//g" $output_file
fi
