package com.yieldbook.mortgage.hbase.action;

import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.hsqldb.lib.StringUtil;

import com.opencsv.CSVParser;
import com.opencsv.CSVReader;
import com.opencsv.CSVWriter;
import com.opencsv.ICSVParser;
import com.yieldbook.mortgage.hbase.bulkimport.FhlmcArmLoanColumnEnum;
import com.yieldbook.mortgage.hbase.bulkimport.FhlmcLoanColumnEnum;
import com.yieldbook.mortgage.hbase.bulkimport.FhlmcModLoanColumnEnum;
import com.yieldbook.mortgage.hbase.operation.BatchOp;

import static com.yieldbook.mortgage.hbase.utility.YBTimeDateCurrencyUtilities.*;

public class FhlmcDailyFileParser extends BaseFileParser {
	
	final static String FHLMC_LOAN_MONTHLY= "fhlmc_loan_monthly";
	final static String FHLMC_ARM_LOAN_MONTHLY = "fhlmc_arm_loan_monthly";
	final static String FHLMC_MOD_LOAN_MONTHLY = "fhlmc_mod_loan_monthly";
	final static byte[] COL_FAM = "m".getBytes();
	final static int LOAN_LENGTH = 48;
	final static int ARM_LOAN_LENGTH = 20;
	final static int MOD_LOAN_LENGTH = 41;

	String inputFileName, poolFileName, asOfDate;
	StringBuilder sb = new StringBuilder();
	BatchOp batchLoan = null;
	BatchOp batchArmLoan = null;
	BatchOp batchModLoan = null;
	CSVWriter loanPen=null;
	CSVWriter armLoanPen=null;
	CSVWriter modLoanPen=null;
	Map<String, String>cusipDictionary=null;
	
	public FhlmcDailyFileParser(String inputFileName, String poolFileName, String asOfDateStr) {

		this.inputFileName = inputFileName;
		this.poolFileName = poolFileName;
		this.asOfDate=asOfDateStr;
		batchLoan = new BatchOp(FHLMC_LOAN_MONTHLY);
		batchArmLoan = new BatchOp(FHLMC_ARM_LOAN_MONTHLY);
		batchModLoan = new BatchOp(FHLMC_MOD_LOAN_MONTHLY);
		if(!StringUtil.isEmpty(poolFileName))
			buildCusipDictionary();
		
		try {
			batchLoan.ConnectToTable();
		} catch (IOException e) {
			e.printStackTrace();
			System.out.println("Cannot conenct to HBase table: " + FHLMC_LOAN_MONTHLY);
		}
		
		try {
			batchArmLoan.ConnectToTable();
		} catch (IOException e) {
			e.printStackTrace();
			System.out.println("Cannot conenct to HBase table: " + FHLMC_ARM_LOAN_MONTHLY);
		}
		
		try {
			batchModLoan.ConnectToTable();
		} catch (IOException e) {
			e.printStackTrace();
			System.out.println("Cannot conenct to HBase table: " + FHLMC_MOD_LOAN_MONTHLY);
		}
		
		try {
			loanPen = new CSVWriter(new FileWriter("fhlmc_loan_daily.csv", true),
					'|', CSVWriter.NO_QUOTE_CHARACTER,
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

    private void insertRow(String[] row, long lastChgDate, String effDate, boolean isArm, boolean isMod) throws IOException {
    	

    	Put putLoan = createLoanPut(row, lastChgDate, effDate);
    	batchLoan.insertPuts(putLoan);
   	
    	if (isArm){
        	Put putArmLoan = createArmLoanPut(row, lastChgDate, effDate);
        	batchArmLoan.insertPuts(putArmLoan);
		} 	
    	
		if (isMod){
        	Put putModLoan = createModLoanPut(row, lastChgDate, effDate);
        	batchModLoan.insertPuts(putModLoan);
		}
    }
    
    private void insertLoanFields(TableTypeEnum tableType, long lastChgDate, String effDate,String[] src, String[] des) {
    	
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
    			des[47]=this.asOfDate; //as_of_date  			
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
    			des[19]=this.asOfDate; //as_of_date 			
    			break;
    		case MOD_LOAN:
    			des[0]=src[0]; //loan_identifier
    			des[1]=src[4]; //cusip
    			des[2]=effDate; //eff_date
    			des[3]=src[1]; //correction_flag
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
    			des[40]=this.asOfDate; //as_of_date
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
		boolean isArm = false;
		boolean isMod = false;
		
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
				isArm = false;
				isMod = false;
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
				String effDate = getEffectiveDate(line[4]);
				insertLoanFields(TableTypeEnum.LOAN,lastChgDate,effDate,line,loanFields);
				loanPen.writeNext(loanFields);
				
		    	if (!StringUtils.isEmpty(line[8])&&line[8].equalsIgnoreCase("ARM")){
		    		//row[8] mapping to Amortization_Type
		    		isArm = true;
		    		if(armLoanPen==null){
		    			armLoanPen = new CSVWriter(new FileWriter("fhlmc_arm_loan_daily.csv", true),
		    					'|', CSVWriter.NO_QUOTE_CHARACTER,
		    					CSVWriter.DEFAULT_ESCAPE_CHARACTER,
		    					CSVWriter.DEFAULT_LINE_END);
		    		}
					String[] loanArmFields = new String[ARM_LOAN_LENGTH];
					insertLoanFields(TableTypeEnum.ARM_LOAN,lastChgDate,effDate,line,loanArmFields);
					armLoanPen.writeNext(loanArmFields);
				} 	
		    	
				if (!StringUtils.isBlank(line[64])
						||!StringUtils.isBlank(line[65])
						||!StringUtils.isBlank(line[66])){
					//row[64] mapping to mod_program 
					//row[65] mapping to mod_type
					//row[66] mapping to num_of_mods
					isMod = true;
					if(modLoanPen==null){
						modLoanPen = new CSVWriter(new FileWriter("fhlmc_mod_loan_daily.csv", true),
								'|', CSVWriter.NO_QUOTE_CHARACTER,
								CSVWriter.DEFAULT_ESCAPE_CHARACTER,
								CSVWriter.DEFAULT_LINE_END);	
					}
					String[] loanModFields = new String[MOD_LOAN_LENGTH];
					insertLoanFields(TableTypeEnum.MOD_LOAN,lastChgDate,effDate,line,loanModFields);
					modLoanPen.writeNext(loanModFields);
				}

				insertRow(line, lastChgDate, effDate, isArm, isMod);
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
		batchLoan.submitBatch();
		batchArmLoan.submitBatch();
		batchModLoan.submitBatch();
	}
	
	private String getEffectiveDate(String cusip){
		String effDate=null;
		if(cusipDictionary==null){
			effDate=this.asOfDate.substring(0,asOfDate.length()-2).concat("01");
		}else{	
			effDate = cusipDictionary.get(cusip);
			if(StringUtils.isEmpty(effDate))
			effDate=this.asOfDate.substring(0,asOfDate.length()-2).concat("01");
		}
		return effDate;
	}
	
	 public Put createLoanPut(String[] fields, long lastChgDate, String effDate){
		 Put put=new Put(Bytes.toBytes(fields[0]));
		 put.addColumn(COL_FAM, FhlmcLoanColumnEnum.CORRECTION_FLAG.getColumnName(), fields[1].getBytes());
		 put.addColumn(COL_FAM, FhlmcLoanColumnEnum.PREFIX.getColumnName(), fields[2].getBytes());
		 put.addColumn(COL_FAM, FhlmcLoanColumnEnum.CUSIP.getColumnName(), fields[3].getBytes());
		 put.addColumn(COL_FAM, FhlmcLoanColumnEnum.ORIG_LOAN_AMT.getColumnName(), fields[5].getBytes());
		 put.addColumn(COL_FAM, FhlmcLoanColumnEnum.ORIG_UPB.getColumnName(), fields[6].getBytes());
		 put.addColumn(COL_FAM, FhlmcLoanColumnEnum.CURRENT_UPB.getColumnName(), fields[7].getBytes());
		 put.addColumn(COL_FAM, FhlmcLoanColumnEnum.PROD_TYPE_IND.getColumnName(), fields[8].getBytes());
		 put.addColumn(COL_FAM, FhlmcLoanColumnEnum.ORIG_NOTE_RATE.getColumnName(), fields[9].getBytes());
		 put.addColumn(COL_FAM, FhlmcLoanColumnEnum.PC_ISSUANCE_NOTE_RATE.getColumnName(), fields[10].getBytes());
		 put.addColumn(COL_FAM, FhlmcLoanColumnEnum.NET_NOTE_RATE.getColumnName(), fields[12].getBytes());
		 put.addColumn(COL_FAM, FhlmcLoanColumnEnum.FIRST_PAYMENT_DATE.getColumnName(), fields[14].getBytes());
		 put.addColumn(COL_FAM, FhlmcLoanColumnEnum.MATURITY_DATE.getColumnName(), fields[15].getBytes());
		 put.addColumn(COL_FAM, FhlmcLoanColumnEnum.ORIG_LOAN_TERM.getColumnName(), fields[16].getBytes());
		 put.addColumn(COL_FAM, FhlmcLoanColumnEnum.REM_MONTHS_TO_MATURITY.getColumnName(), fields[17].getBytes());
		 put.addColumn(COL_FAM, FhlmcLoanColumnEnum.LOAN_AGE.getColumnName(), fields[18].getBytes());
		 put.addColumn(COL_FAM, FhlmcLoanColumnEnum.ORIG_LTV.getColumnName(), fields[19].getBytes());
		 put.addColumn(COL_FAM, FhlmcLoanColumnEnum.ORIG_CLTV.getColumnName(), fields[20].getBytes());
		 put.addColumn(COL_FAM, FhlmcLoanColumnEnum.ORIG_DTI.getColumnName(), fields[21].getBytes());
		 put.addColumn(COL_FAM, FhlmcLoanColumnEnum.CREDIT_SCORE.getColumnName(), fields[22].getBytes());
		 put.addColumn(COL_FAM, FhlmcLoanColumnEnum.NUM_BORROWERS.getColumnName(), fields[26].getBytes());
		 put.addColumn(COL_FAM, FhlmcLoanColumnEnum.FIRST_TIME_BUYER.getColumnName(), fields[27].getBytes());
		 put.addColumn(COL_FAM, FhlmcLoanColumnEnum.LOAN_PURPOSE.getColumnName(), fields[28].getBytes());
		 put.addColumn(COL_FAM, FhlmcLoanColumnEnum.OCCUPANCY_STATUS.getColumnName(), fields[29].getBytes());
		 put.addColumn(COL_FAM, FhlmcLoanColumnEnum.NUM_UNITS.getColumnName(), fields[30].getBytes());
		 put.addColumn(COL_FAM, FhlmcLoanColumnEnum.PROPERTY_TYPE.getColumnName(), fields[31].getBytes());
		 put.addColumn(COL_FAM, FhlmcLoanColumnEnum.TPO_FLAG.getColumnName(), fields[32].getBytes());
		 put.addColumn(COL_FAM, FhlmcLoanColumnEnum.STATE.getColumnName(), fields[33].getBytes());
		 put.addColumn(COL_FAM, FhlmcLoanColumnEnum.SELLER.getColumnName(), fields[34].getBytes());
		 put.addColumn(COL_FAM, FhlmcLoanColumnEnum.SERVICER.getColumnName(), fields[35].getBytes());
		 put.addColumn(COL_FAM, FhlmcLoanColumnEnum.INS_PERCENT.getColumnName(), fields[36].getBytes());
		 put.addColumn(COL_FAM, FhlmcLoanColumnEnum.INS_CANCEL_IND.getColumnName(), fields[37].getBytes());
		 put.addColumn(COL_FAM, FhlmcLoanColumnEnum.GOVT_INS_GRNTE.getColumnName(), fields[38].getBytes());
		 put.addColumn(COL_FAM, FhlmcLoanColumnEnum.ASSUMABILITY_IND.getColumnName(), fields[39].getBytes());
		 put.addColumn(COL_FAM, FhlmcLoanColumnEnum.IO_FLAG.getColumnName(), fields[40].getBytes());
		 put.addColumn(COL_FAM, FhlmcLoanColumnEnum.FIRST_PI_DATE.getColumnName(), fields[41].getBytes());
		 put.addColumn(COL_FAM, FhlmcLoanColumnEnum.MONTHS_TO_AMORTIZE.getColumnName(), fields[42].getBytes());
		 put.addColumn(COL_FAM, FhlmcLoanColumnEnum.PREPAY_PENALTY_FLAG.getColumnName(), fields[43].getBytes());
		 put.addColumn(COL_FAM, FhlmcLoanColumnEnum.PREPAY_TERM.getColumnName(), fields[44].getBytes());
		 put.addColumn(COL_FAM, FhlmcLoanColumnEnum.ESTM_LTV.getColumnName(), fields[72].getBytes());
		 put.addColumn(COL_FAM, FhlmcLoanColumnEnum.UPD_CREDIT_SCORE.getColumnName(), fields[73].getBytes());
		 put.addColumn(COL_FAM, FhlmcLoanColumnEnum.EFF_DATE.getColumnName(), effDate.getBytes());
		 put.addColumn(COL_FAM, FhlmcLoanColumnEnum.AS_OF_DATE.getColumnName(), this.asOfDate.getBytes());
		 
	     String lastChgDateStr = lastChgDate + "";
		 put.addColumn(COL_FAM, FhlmcLoanColumnEnum.LAST_CHG_DATE.getColumnName(), lastChgDateStr.getBytes());
		 
		 return put;
	 }	
	 
	 public Put createArmLoanPut(String[] fields, long lastChgDate, String effDate){
		 Put put=new Put(Bytes.toBytes(fields[0]));

		 put.addColumn(COL_FAM, FhlmcArmLoanColumnEnum.CUSIP.getColumnName(), fields[3].getBytes());
		 put.addColumn(COL_FAM, FhlmcArmLoanColumnEnum.PRODUCT_TYPE.getColumnName(), fields[8].getBytes());
		 put.addColumn(COL_FAM, FhlmcArmLoanColumnEnum.INDEX_DESC.getColumnName(), fields[45].getBytes());
		 put.addColumn(COL_FAM, FhlmcArmLoanColumnEnum.GROSS_MARGIN.getColumnName(), fields[46].getBytes());
		 put.addColumn(COL_FAM, FhlmcArmLoanColumnEnum.NET_MARGIN.getColumnName(), fields[47].getBytes());
		 put.addColumn(COL_FAM, FhlmcArmLoanColumnEnum.RATE_ADJMT_FREQ.getColumnName(), fields[48].getBytes());
		 put.addColumn(COL_FAM, FhlmcArmLoanColumnEnum.LOOKBACK.getColumnName(), fields[49].getBytes());
		 put.addColumn(COL_FAM, FhlmcArmLoanColumnEnum.CONVERTIBLE_FLAG.getColumnName(), fields[52].getBytes());
		 put.addColumn(COL_FAM, FhlmcArmLoanColumnEnum.INITIAL_PERIOD.getColumnName(), fields[53].getBytes());
		 put.addColumn(COL_FAM, FhlmcArmLoanColumnEnum.NEXT_ADJMT_DATE.getColumnName(), fields[54].getBytes());
		 put.addColumn(COL_FAM, FhlmcArmLoanColumnEnum.MONTHS_TO_ADJUST.getColumnName(), fields[55].getBytes());
		 put.addColumn(COL_FAM, FhlmcArmLoanColumnEnum.MAX_LIFE_RATE.getColumnName(), fields[56].getBytes());
		 put.addColumn(COL_FAM, FhlmcArmLoanColumnEnum.NET_MAX_LIFE_RATE.getColumnName(), fields[57].getBytes());
		 put.addColumn(COL_FAM, FhlmcArmLoanColumnEnum.INIT_CAP_UP.getColumnName(), fields[60].getBytes());
		 put.addColumn(COL_FAM, FhlmcArmLoanColumnEnum.INIT_CAP_DN.getColumnName(), fields[61].getBytes());
		 put.addColumn(COL_FAM, FhlmcArmLoanColumnEnum.PERIODIC_CAP.getColumnName(), fields[62].getBytes());
		 put.addColumn(COL_FAM, FhlmcArmLoanColumnEnum.EFF_DATE.getColumnName(), effDate.getBytes());		 
		 put.addColumn(COL_FAM, FhlmcArmLoanColumnEnum.AS_OF_DATE.getColumnName(), this.asOfDate.getBytes());	
		 
	     String lastChgDateStr = lastChgDate + "";
		 put.addColumn(COL_FAM, FhlmcLoanColumnEnum.LAST_CHG_DATE.getColumnName(), lastChgDateStr.getBytes());
		 
		 return put;
	 }
	 
	 public Put createModLoanPut(String[] fields, long lastChgDate, String effDate){
		 Put put=new Put(Bytes.toBytes(fields[0]));

		 put.addColumn(COL_FAM, FhlmcModLoanColumnEnum.CORRECTION_FLAG.getColumnName(), fields[1].getBytes());
		 put.addColumn(COL_FAM, FhlmcModLoanColumnEnum.CUSIP.getColumnName(), fields[3].getBytes());
		 put.addColumn(COL_FAM, FhlmcModLoanColumnEnum.PRODUCT_TYPE.getColumnName(), fields[8].getBytes());
		 put.addColumn(COL_FAM, FhlmcModLoanColumnEnum.CUR_GROSS_NOTE_RATE.getColumnName(), fields[13].getBytes());
		 put.addColumn(COL_FAM, FhlmcModLoanColumnEnum.MOD_PROGRAM.getColumnName(), fields[64].getBytes());
		 put.addColumn(COL_FAM, FhlmcModLoanColumnEnum.MOD_TYPE.getColumnName(), fields[65].getBytes());
		 put.addColumn(COL_FAM, FhlmcModLoanColumnEnum.NUM_OF_MODS.getColumnName(), fields[66].getBytes());
		 put.addColumn(COL_FAM, FhlmcModLoanColumnEnum.TOT_CAPITALIZED_AMT.getColumnName(), fields[67].getBytes());
		 put.addColumn(COL_FAM, FhlmcModLoanColumnEnum.INT_BEAR_LOAN_AMT.getColumnName(), fields[68].getBytes());
		 put.addColumn(COL_FAM, FhlmcModLoanColumnEnum.DEFERRED_AMT.getColumnName(), fields[69].getBytes());
		 put.addColumn(COL_FAM, FhlmcModLoanColumnEnum.DEFERRED_UPB.getColumnName(), fields[70].getBytes());
		 put.addColumn(COL_FAM, FhlmcModLoanColumnEnum.MOD_DATE_LOAN_AGE.getColumnName(), fields[71].getBytes());
		 put.addColumn(COL_FAM, FhlmcModLoanColumnEnum.RATE_STEP_IND.getColumnName(), fields[75].getBytes());
		 put.addColumn(COL_FAM, FhlmcModLoanColumnEnum.INITIAL_FIXED_PER.getColumnName(), fields[76].getBytes());
		 put.addColumn(COL_FAM, FhlmcModLoanColumnEnum.TOT_STEPS.getColumnName(), fields[77].getBytes());
		 put.addColumn(COL_FAM, FhlmcModLoanColumnEnum.REM_STEPS.getColumnName(), fields[78].getBytes());
		 put.addColumn(COL_FAM, FhlmcModLoanColumnEnum.NEXT_STEP_RATE.getColumnName(), fields[79].getBytes());
		 put.addColumn(COL_FAM, FhlmcModLoanColumnEnum.TERMINAL_STEP_RATE.getColumnName(), fields[80].getBytes());
		 put.addColumn(COL_FAM, FhlmcModLoanColumnEnum.TERMINAL_STEP_DATE.getColumnName(), fields[81].getBytes());
		 put.addColumn(COL_FAM, FhlmcModLoanColumnEnum.RATE_ADJ_FREQ.getColumnName(), fields[82].getBytes());
		 put.addColumn(COL_FAM, FhlmcModLoanColumnEnum.NEXT_ADJ_DATE.getColumnName(), fields[83].getBytes());
		 put.addColumn(COL_FAM, FhlmcModLoanColumnEnum.MONTHS_TO_ADJ.getColumnName(), fields[84].getBytes());
		 put.addColumn(COL_FAM, FhlmcModLoanColumnEnum.PERIODIC_CAP_UP.getColumnName(), fields[85].getBytes());
		 put.addColumn(COL_FAM, FhlmcModLoanColumnEnum.ORIGIN_LOAN_AMT.getColumnName(), fields[86].getBytes());
		 put.addColumn(COL_FAM, FhlmcModLoanColumnEnum.ORIGIN_NOTE_RATE.getColumnName(), fields[87].getBytes());
		 put.addColumn(COL_FAM, FhlmcModLoanColumnEnum.ORIGIN_PRODUCT_TYPE.getColumnName(), fields[88].getBytes());
		 put.addColumn(COL_FAM, FhlmcModLoanColumnEnum.ORIGIN_IO_FLAG.getColumnName(), fields[89].getBytes());
		 put.addColumn(COL_FAM, FhlmcModLoanColumnEnum.ORIGIN_FIRST_PAYM_DATE.getColumnName(), fields[90].getBytes());
		 put.addColumn(COL_FAM, FhlmcModLoanColumnEnum.ORIGIN_MATURITY_DATE.getColumnName(), fields[91].getBytes());
		 put.addColumn(COL_FAM, FhlmcModLoanColumnEnum.ORIGIN_LOAN_TERM.getColumnName(), fields[92].getBytes());
		 put.addColumn(COL_FAM, FhlmcModLoanColumnEnum.ORIGIN_LTV.getColumnName(), fields[93].getBytes());
		 put.addColumn(COL_FAM, FhlmcModLoanColumnEnum.ORIGIN_CLTV.getColumnName(), fields[94].getBytes());
		 put.addColumn(COL_FAM, FhlmcModLoanColumnEnum.ORIGIN_DTI.getColumnName(), fields[95].getBytes());
		 put.addColumn(COL_FAM, FhlmcModLoanColumnEnum.ORIGIN_CREDIT_SCORE.getColumnName(), fields[96].getBytes());
		 put.addColumn(COL_FAM, FhlmcModLoanColumnEnum.ORIGIN_LOAN_PURPOSE.getColumnName(), fields[100].getBytes());
		 put.addColumn(COL_FAM, FhlmcModLoanColumnEnum.ORIGIN_OCCUPANCY_STATUS.getColumnName(), fields[101].getBytes());
		 put.addColumn(COL_FAM, FhlmcModLoanColumnEnum.ORIGIN_TPO_FLAG.getColumnName(), fields[102].getBytes());
		 put.addColumn(COL_FAM, FhlmcModLoanColumnEnum.EFF_DATE.getColumnName(), effDate.getBytes());
		 put.addColumn(COL_FAM, FhlmcModLoanColumnEnum.AS_OF_DATE.getColumnName(), this.asOfDate.getBytes());	
		 
	     String lastChgDateStr = lastChgDate + "";
		 put.addColumn(COL_FAM, FhlmcLoanColumnEnum.LAST_CHG_DATE.getColumnName(), lastChgDateStr.getBytes());

		 return put;
	 }

	private void buildCusipDictionary() {
		cusipDictionary = new HashMap<>();
		CSVReader reader = null;
		String[] line;
		try {
			reader = new CSVReader(new FileReader(poolFileName), ' ',
					CSVParser.DEFAULT_QUOTE_CHARACTER,
					ICSVParser.DEFAULT_ESCAPE_CHARACTER,
					CSVReader.DEFAULT_SKIP_LINES,
					ICSVParser.DEFAULT_STRICT_QUOTES,
					ICSVParser.DEFAULT_IGNORE_LEADING_WHITESPACE);

			while ((line = reader.readNext()) != null && line.length >= 2
					&& !StringUtils.isEmpty(line[0])
					&& !StringUtils.isEmpty(line[1])) {
				cusipDictionary.put(line[0], convertMMyyyDataFormat(line[1]));
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	} 
}
