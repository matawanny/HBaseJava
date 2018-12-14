package com.yieldbook.mortgage.hbase.process;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang3.StringUtils;

import com.yieldbook.mortgage.hbase.action.BaseFileParser;
import com.yieldbook.mortgage.hbase.action.FhlmcDailyFileParser;
import com.yieldbook.mortgage.hbase.action.FnmaDailyFileParser;
import com.yieldbook.mortgage.hbase.action.GnmaDailyFileParser;

public class DailyProcess {
	
	private BaseFileParser baseFileParser;

	
	public DailyProcess(BaseFileParser baseFileParser) {
		super();
		this.baseFileParser = baseFileParser;
	}

	public void parseAndWriteFile(){
		baseFileParser.parseAndWriteFile();
	}

	public static void main(String[] args) throws Exception {

		  Options options = new Options();

		  Option input = new Option("i", "input", true, "input file to read data from");
		  input.setRequired(true);
		  input.setArgName("FILE INPUT PATH");
		  options.addOption(input);
		  
		  Option filetype = new Option("t", "filetype", true, "input file type, should be fhlmc or fhlmcold or fnma or gnma or fhlmcdaily or fnmadaily ");
		  filetype.setRequired(true);
		  filetype.setArgName("FILE TYPE");
		  options.addOption(filetype);

		  Option output = new Option("o", "output", true, "output file to write the final result");
		  //output.setRequired(true);
		  output.setArgName("FILE OUT PATH");
		  options.addOption(output);

		  Option asofdate = new Option("d", "asofdate", true, "As Of Date from *.SIG file");
		  //output.setRequired(true);
		  asofdate.setArgName("AS OF DATE");
		  options.addOption(asofdate);
		  
		  Option lastchgdate = new Option("l", "lastchgdate", true, "Last Change Date related to storing folder name");
		  //output.setRequired(true);
		  asofdate.setArgName("LAST CHANGE DATE");
		  options.addOption(lastchgdate);
		  
		  Option poolfile = new Option("p", "poolfile", true, "Pool Level source file like FHLSEC_DIC.TXT");
		  //output.setRequired(true);
		  asofdate.setArgName("POOL FILE");
		  options.addOption(poolfile);
		  
		  CommandLineParser commandLineParser = new DefaultParser();
		  HelpFormatter helpFormatter = new HelpFormatter();
		  CommandLine commandLine;

		  try {
		   commandLine = commandLineParser.parse(options, args);
		  } catch (ParseException e) {
		   System.out.println(e.getMessage());
		   helpFormatter.printHelp("Preprocess", options);

		   System.exit(1);
		   return;
		  }

		  String inputFileStr = commandLine.getOptionValue("input");
		  String fileTypeStr = commandLine.getOptionValue("filetype");
		  String outputFileStr = commandLine.getOptionValue("output");
		  String poolFileStr = commandLine.getOptionValue("poolfile");
		  String asOfDateStr = commandLine.getOptionValue("asofdate");
		  String lastChgDateStr = commandLine.getOptionValue("lastchgdate");

		  System.out.println("Input File: " + inputFileStr);
		  System.out.println("filetype : " + fileTypeStr);
		  if(!StringUtils.isEmpty(outputFileStr))
			  System.out.println("Output File Path : " + outputFileStr);
		  if(!StringUtils.isEmpty(asOfDateStr))
			  System.out.println("As Of Date : " + asOfDateStr);
		  if(!StringUtils.isEmpty(lastChgDateStr))
			  System.out.println("LAST CHANGE Date : " + lastChgDateStr);			  

		  
		  DailyProcess preProcess=null;
		  switch(fileTypeStr){
		  case "fhlmc_loan":
			  preProcess = new DailyProcess(new FhlmcDailyFileParser(inputFileStr,poolFileStr, asOfDateStr));
			  preProcess.parseAndWriteFile();
			  break;
		  case "fnma_loan":
			  preProcess = new DailyProcess(new FnmaDailyFileParser(inputFileStr, asOfDateStr));
			  preProcess.parseAndWriteFile();
			  break;
		  case "gnma_loan":
			  preProcess = new DailyProcess(new GnmaDailyFileParser(inputFileStr, asOfDateStr,  lastChgDateStr));
			  preProcess.parseAndWriteFile();
			  break;			  
		  default: 
			  System.out.println("File Type: " +  fileTypeStr + " is not supported. ");
		  }

		 }
}
