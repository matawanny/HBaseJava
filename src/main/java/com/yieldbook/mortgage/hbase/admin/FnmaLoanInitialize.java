package com.yieldbook.mortgage.hbase.admin;


import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.util.Bytes;

public class FnmaLoanInitialize {

	final static String FNMA_LOAN="fnma_loan_monthly";
	final static String FNMA_ARM_LOAN="fnma_arm_loan_monthly";
	final static String FNMA_MOD_LOAN="fnma_mod_loan_monthly";
	
    public static void main(String[] args) throws IOException {

        Configuration conf = HBaseConfiguration.create();

        Connection connection = ConnectionFactory.createConnection(conf);

        Admin admin = connection.getAdmin();
        
        String[] tables = new String[3];
        tables[0]=FNMA_LOAN;
        tables[1]=FNMA_ARM_LOAN;
        tables[2]=FNMA_MOD_LOAN;
        
        if(args.length<5){
        	System.out.print("Must have 5 region split keys!");
        	System.exit(1);
        }
        
        byte[][] regions = new byte[][] { //CreateTableWithRegionsExample-5-Regions Manually create region split keys.
        	      Bytes.toBytes(args[0]),
        	      Bytes.toBytes(args[1]),
        	      Bytes.toBytes(args[2]),
        	      Bytes.toBytes(args[3]),
        	      Bytes.toBytes(args[4])
        	    };
        
        for(String table: tables){
	        HTableDescriptor tableName = new HTableDescriptor(TableName.valueOf(table));
	        if (admin.tableExists(tableName.getTableName())) {
	            admin.disableTable(tableName.getTableName());
	            admin.deleteTable(tableName.getTableName());
	            System.out.println("Table " + table + " has been deleted.");
	        }
        }
        
        for(String table: tables){
	        HTableDescriptor tableName = new HTableDescriptor(TableName.valueOf(table));
	        tableName.addFamily(new HColumnDescriptor("m"));
	        if (!admin.tableExists(tableName.getTableName())) {
	            admin.createTable(tableName,regions);
	            System.out.println("Table " + table + " has been created.");
	        }
        }
      
    }
}
