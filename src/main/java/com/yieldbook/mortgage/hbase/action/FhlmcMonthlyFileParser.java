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
import static com.yieldbook.mortgage.hbase.utility.YBTimeDateCurrencyUtilities.*;

public class FhlmcMonthlyFileParser extends BaseFileParser {
	
	final static int LOAN_LENGTH = 48;
	final static int ARM_LOAN_LENGTH = 20;
	final static int MOD_LOAN_LENGTH = 41;

	String inputFileName;
	StringBuilder sb = new StringBuilder();
	CSVWriter loanPen=null;
	CSVWriter armLoanPen=null;
	CSVWriter modLoanPen=null;
	String afOfDateMonthly;
	public FhlmcMonthlyFileParser(String inputFileName, String  asOfDateStr) {
		super();
		this.inputFileName = inputFileName;
		this.afOfDateMonthly = asOfDateStr;
		
		try {
			loanPen = new CSVWriter(new FileWriter("fhlmc_loan_monthly.csv", true),
					'|', CSVWriter.NO_QUOTE_CHARACTER,
					CSVWriter.DEFAULT_ESCAPE_CHARACTER,
					CSVWriter.DEFAULT_LINE_END);
			armLoanPen = new CSVWriter(new FileWriter(
					"fhlmc_arm_loan_monthly.csv", true), '|',
					CSVWriter.NO_QUOTE_CHARACTER,
					CSVWriter.DEFAULT_ESCAPE_CHARACTER,
					CSVWriter.DEFAULT_LINE_END);
			modLoanPen = new CSVWriter(new FileWriter(
					"fhlmc_mod_loan_monthly.csv", true), '|',
					CSVWriter.NO_QUOTE_CHARACTER,
					CSVWriter.DEFAULT_ESCAPE_CHARACTER,
					CSVWriter.DEFAULT_LINE_END);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	

	public void printLine(String[] line){
		if (line.length==1){
			System.out.println(line[0]);
		}else{
			for (int i=0; i<line.length-2; i++)
				System.out.print(line[i] + "|");
			
			System.out.println(line[line.length-1]);
		}// end of else
	}


    private void insertLoanFields(TableTypeEnum tableType, long lastChgDate, String[] src, String[] des) {
    	String effDate=this.afOfDateMonthly.substring(0,6)+"01";
    	
    	switch(tableType) {
    		case LOAN:
    			des[0]=src[0]; //loan_identifier
    			des[1]=src[4]; //cusip
    			des[2]=src[8]; //prod_type_ind
    			des[3]=src[28]; //loan_purpose
    			des[4]=src[32]; //tpo_flag
    			des[5]=src[31]; //property_type
    			des[6]=src[29]; //occupancy_status
    			des[7]=src[30]; //num_units
    			des[8]=src[33]; //state
    			des[9]=src[22]; //credit_score
    			des[10]=src[16]; //orig_loan_term
    			des[11]=src[19]; //orig_ltv
    			des[12]=src[43]; //prepay_penalty_flag
    			des[13]=src[40]; //io_flag
    			des[14]=src[14]; //first_payment_date
    			des[15]=src[41]; //first_pi_date
    			des[16]=src[15]; //maturity_date
    			des[17]=src[9]; //orig_note_rate
    			des[18]=src[10]; //pc_issuance_note_rate
    			des[19]=src[12]; //net_note_rate
    			des[20]=src[5]; //orig_loan_amt
    			des[21]=src[6]; //orig_upb
    			des[22]=src[18]; //loan_age
    			des[23]=src[17]; //rem_months_to_maturity
    			des[24]=src[42]; //months_to_amortize
    			des[25]=src[35]; //servicer
    			des[26]=src[34]; //seller
    			des[27]=lastChgDate+""; //last_chg_date
    			des[28]=src[7]; //current_upb
    			des[29]=effDate; //eff_date
    			des[30]=""; //doc_assets
    			des[31]=""; //doc_empl
    			des[32]=""; //doc_income
    			des[33]=src[20]; //orig_cltv
    			des[34]=src[26]; //num_borrowers
    			des[35]=src[27]; //first_time_buyer
    			des[36]=src[36]; //ins_percent
    			des[37]=src[21]; //orig_dti
    			des[38]=""; //msa
    			des[39]=src[73]; //upd_credit_score
    			des[40]=src[72]; //estm_ltv
    			des[41]=src[1]; //correction_flag
    			des[42]=src[2]; //prefix
    			des[43]=src[37]; //ins_cancel_ind
    			des[44]=src[38]; //govt_ins_grnte
    			des[45]=src[39]; //assumability_ind
    			des[46]=src[44]; //prepay_term
    			des[47]=this.afOfDateMonthly; //as_of_date  			
    			break;
    		case ARM_LOAN:
    			des[0]=src[0]; //loan_identifier
    			des[1]=src[4]; //cusip
    			des[2]=src[8]; //product_type
    			des[3]=src[52]; //convertible_flag
    			des[4]=src[48]; //rate_adjmt_freq
    			des[5]=src[53]; //initial_period
    			des[6]=src[54]; //next_adjmt_date
    			des[7]=src[49]; //lookback
    			des[8]=src[46]; //gross_margin
    			des[9]=src[47]; //net_margin
    			des[10]=src[57]; //net_max_life_rate
    			des[11]=src[56]; //max_life_rate
    			des[12]=src[60]; //init_cap_up
    			des[13]=src[61]; //init_cap_dn
    			des[14]=src[62]; //periodic_cap
    			des[15]=src[55]; //months_to_adjust
    			des[16]=src[45]; //index_desc
    			des[17]=lastChgDate+""; //last_chg_date
    			des[18]=effDate; //eff_date
    			des[19]=this.afOfDateMonthly; //as_of_date 			
    			break;
    		case MOD_LOAN:
    			des[0]=src[0]; //loan_identifier
    			des[1]=src[4]; //cusip
    			des[2]=effDate; //eff_date
    			des[3]=""; //correction_flag. Monthly files use null as default
    			des[4]=src[8]; //product_type
    			des[5]=src[100]; //origin_loan_purpose
    			des[6]=src[102]; //origin_tpo_flag
    			des[7]=src[101]; //origin_occupancy_status
    			des[8]=src[96]; //origin_credit_score
    			des[9]=src[92]; //origin_loan_term
    			des[10]=src[93]; //origin_ltv
    			des[11]=src[89]; //origin_io_flag
    			des[12]=src[90]; //origin_first_paym_date
    			des[13]=src[91]; //origin_maturity_date
    			des[14]=src[87]; //origin_note_rate
    			des[15]=src[86]; //origin_loan_amt
    			des[16]=src[94]; //origin_cltv
    			des[17]=src[95]; //origin_dti
    			des[18]=src[88]; //origin_product_type
    			des[19]=src[71]; //mod_date_loan_age
    			des[20]=src[64]; //mod_program
    			des[21]=src[65]; //mod_type
    			des[22]=src[66]; //num_of_mods
    			des[23]=src[67]; //tot_capitalized_amt
    			des[24]=lastChgDate+""; //last_chg_date
    			des[25]=src[68]; //int_bear_loan_amt
    			des[26]=src[69]; //deferred_amt
    			des[27]=src[70]; //deferred_upb
    			des[28]=src[75]; //rate_step_ind
    			des[29]=src[77]; //tot_steps
    			des[30]=src[78]; //rem_steps
    			des[31]=src[76]; //initial_fixed_per
    			des[32]=src[82]; //rate_adj_freq
    			des[33]=src[85]; //periodic_cap_up
    			des[34]=src[84]; //months_to_adj
    			des[35]=src[79]; //next_step_rate
    			des[36]=src[83]; //next_adj_date
    			des[37]=src[80]; //terminal_step_rate
    			des[38]=src[81]; //terminal_step_date
    			des[39]=src[13]; //cur_gross_note_rate
    			des[40]=this.afOfDateMonthly; //as_of_date
    			break;
    		default:
    		    throw new AssertionError("Unknown Table Type");
    	
    	}
    }
	
	@Override
	public void parseAndWriteFile() {

		CSVReader reader = null;
		String[] line =null;
		long lastChgDate = Calendar.getInstance().getTimeInMillis();
		
		
		try {
			reader = new CSVReader(new FileReader(inputFileName), '|',
					CSVParser.DEFAULT_QUOTE_CHARACTER,
					ICSVParser.DEFAULT_ESCAPE_CHARACTER,
					CSVReader.DEFAULT_SKIP_LINES,
					ICSVParser.DEFAULT_STRICT_QUOTES,
					ICSVParser.DEFAULT_IGNORE_LEADING_WHITESPACE);
            
			while ((line = reader.readNext()) != null) {
				if (StringUtils.isEmpty(line[0]))
					continue;
				if(!StringUtils.isEmpty(line[14]))
					line[14]=getMonthYearMillionSecsEmbs(line[14]);
				if(!StringUtils.isEmpty(line[15]))
					line[15]=getMonthYearMillionSecsEmbs(line[15]);					
				if(!StringUtils.isEmpty(line[41]))
					line[41]=getMonthYearMillionSecsEmbs(line[41]);					
				if(!StringUtils.isEmpty(line[54]))
					line[54]=getMonthYearMillionSecsEmbs(line[54]);
				if(!StringUtils.isEmpty(line[81]))
					line[81]=getMonthYearMillionSecsEmbs(line[81]);
				if(!StringUtils.isEmpty(line[83]))
					line[83]=getMonthYearMillionSecsEmbs(line[83]);
				if(!StringUtils.isEmpty(line[90]))
					line[90]=getMonthYearMillionSecsEmbs(line[90]);
				if(!StringUtils.isEmpty(line[91]))
					line[91]=getMonthYearMillionSecsEmbs(line[91]);	
				
				String[] loanFields = new String[LOAN_LENGTH];
				insertLoanFields(TableTypeEnum.LOAN,lastChgDate,line,loanFields);
				loanPen.writeNext(loanFields);
				
		    	if (!StringUtils.isEmpty(line[8])&&line[8].equalsIgnoreCase("ARM")){
		    		//row[8] mapping to Amortization_Type
					String[] loanArmFields = new String[ARM_LOAN_LENGTH];
					insertLoanFields(TableTypeEnum.ARM_LOAN,lastChgDate,line,loanArmFields);
					armLoanPen.writeNext(loanArmFields);
				} 	
		    	
				if (!StringUtils.isBlank(line[64])
						||!StringUtils.isBlank(line[65])
						||!StringUtils.isBlank(line[66])){
					//row[64] mapping to mod_program 
					//row[65] mapping to mod_type
					//row[66] mapping to num_of_mods
					String[] loanModFields = new String[MOD_LOAN_LENGTH];
					insertLoanFields(TableTypeEnum.MOD_LOAN,lastChgDate,line,loanModFields);
					modLoanPen.writeNext(loanModFields);
				}
			} // end of While
			if(loanPen!=null)
				loanPen.close();
			if(armLoanPen!=null)
				armLoanPen.close();
			if(modLoanPen!=null)
				modLoanPen.close();
		} catch (IOException e) {
			System.out.println("Cannot parse line + \"" + line +"\"");
			e.printStackTrace();
		}
	}
}
