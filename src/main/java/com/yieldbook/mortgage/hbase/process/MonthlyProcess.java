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

public class MonthlyProcess {
	
	private BaseFileParser baseFileParser;

	
	public MonthlyProcess(BaseFileParser baseFileParser) {
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
		  String asOfDateStr = commandLine.getOptionValue("asofdate");

		  System.out.println("Input File: " + inputFileStr);
		  System.out.println("filetype : " + fileTypeStr);
		  if(!StringUtils.isEmpty(outputFileStr))
			  System.out.println("Output File Path : " + outputFileStr);
		  if(!StringUtils.isEmpty(asOfDateStr))
			  System.out.println("As Of Date : " + asOfDateStr);
		  fileTypeStr = fileTypeStr.toLowerCase();
		  
		  MonthlyProcess preProcess=null;
		  switch(fileTypeStr){
		  case "fhlmc":
			  preProcess = new MonthlyProcess(new FhlmcDailyFileParser(inputFileStr));
			  preProcess.parseAndWriteFile();
			  break;
		  case "fnma":
			  /*preProcess = new DailyProcess(new FNMAFileParser(inputFileStr, asOfDateStr, outputFileStr));
			  preProcess.parseAndWriteFile();*/
			  break;
			  
		  default: 
			  System.out.println("File Type: " +  fileTypeStr + " is not supported. ");
		  
		  }

		 }
}
