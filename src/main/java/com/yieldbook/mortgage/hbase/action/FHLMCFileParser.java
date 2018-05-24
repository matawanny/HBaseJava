package com.yieldbook.mortgage.hbase.action;

import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;

import com.opencsv.CSVParser;
import com.opencsv.CSVReader;
import com.opencsv.ICSVParser;
import com.yieldbook.mortgage.hbase.operation.BatchOp;
import com.yieldbook.mortgage.hbase.utility.FhlmcDic;
import com.yieldbook.mortgage.hbase.utility.YBTimeDateCurrencyUtilities;

public class FHLMCFileParser extends BaseFileParser {

	String inputFileName;
	Long epochMilliSecondsAsOfDate;
	StringBuilder sb = new StringBuilder();
	BatchOp batch = null;

	
	public FHLMCFileParser(String inputFileName, String asOfDate) {
		super();
		this.inputFileName = inputFileName;
		try {
			this.epochMilliSecondsAsOfDate=YBTimeDateCurrencyUtilities
					.getMillionSeconds(asOfDate);
		} catch (ParseException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
		batch = new BatchOp(FhlmcDic.FHLMC_HBASE_TABLE);
		try {
			batch.ConnectToTable();
		} catch (IOException e) {
			e.printStackTrace();
			System.out.println("Cannot conenct to HBase table: " + FhlmcDic.FHLMC_HBASE_TABLE);
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

    private void insertRow(String[] row) throws IOException {
    	
    	if(row==null || row.length!=FhlmcDic.Columns.length)
    	   throw new IOException("The columns number is wrong");
    	
    	if (row[1].equalsIgnoreCase("D")){
    		Delete delete = FhlmcDic.createDelete(row[0]);
    		batch.insertDeletes(delete);
    	}else{
    		Put put = FhlmcDic.createPut(row);
    		batch.insertPuts(put);
    	}
    }
	
	@Override
	public void parseAndWriteFile() {

		CSVReader reader = null;
		String[] line;
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
				String[] entries = new String[line.length+1];
				if(!StringUtils.isEmpty(line[14]))
					line[14]=YBTimeDateCurrencyUtilities
						.getMonthYearMillionSecsFHLM(line[14]) + "";
				if(!StringUtils.isEmpty(line[15]))
					line[15]=YBTimeDateCurrencyUtilities
						.getMonthYearMillionSecsFHLM(line[15]) + "";					
				if(!StringUtils.isEmpty(line[41]))
					line[41]=YBTimeDateCurrencyUtilities
						.getMonthYearMillionSecsFHLM(line[41]) + "";					
				if(!StringUtils.isEmpty(line[54]))
					line[54]=YBTimeDateCurrencyUtilities
						.getMonthYearMillionSecsFHLM(line[54]) + "";
				if(!StringUtils.isEmpty(line[81]))
					line[81]=YBTimeDateCurrencyUtilities
						.getMonthYearMillionSecsFHLM(line[81]) + "";
				if(!StringUtils.isEmpty(line[83]))
					line[83]=YBTimeDateCurrencyUtilities
						.getMonthYearMillionSecsFHLM(line[83]) + "";
				if(!StringUtils.isEmpty(line[90]))
					line[90]=YBTimeDateCurrencyUtilities
						.getMonthYearMillionSecsFHLM(line[90]) + "";
				if(!StringUtils.isEmpty(line[91]))
					line[91]=YBTimeDateCurrencyUtilities
						.getMonthYearMillionSecsFHLM(line[91]) + "";	
				System.arraycopy(line, 0, entries, 0, line.length);
				entries[entries.length-1]=epochMilliSecondsAsOfDate + "";
				insertRow(entries);
			} // end of While
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ParseException e1) {
			System.out
					.println("The below line cannot be parsed to AsOfDate.");

		}
		batch.submitBatch();
	}

}
