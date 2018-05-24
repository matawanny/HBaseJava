package com.yieldbook.mortgage.hbase.admin;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;

public class createTable   {
	
	public static final String HBASE_CONFIGURATION_ZOOKEEPER_QUORUM="hbase.zookeeper.quorum";
	public static final String HBASE_CONFIGURATION_ZOOKEEPER_CLIENTPORT="hbase.zookeeper.property.clientPort";

    public static void main(String[] args) throws IOException {

        Configuration conf = HBaseConfiguration.create();
    	Configuration hConf = HBaseConfiguration.create(conf);
/*    	hConf.set(HBASE_CONFIGURATION_ZOOKEEPER_QUORUM, "ybgdev96.ny.ssmb.com,ybrdev93.ny.ssmb.com,ybrdev96.ny.ssmb.com");
    	hConf.set(HBASE_CONFIGURATION_ZOOKEEPER_CLIENTPORT, "2181");
*/
        Connection connection = ConnectionFactory.createConnection(conf);

        Admin admin = connection.getAdmin();

        HTableDescriptor tableName = new HTableDescriptor(TableName.valueOf("notifications"));

        tableName.addFamily(new HColumnDescriptor("attributes"));
        tableName.addFamily(new HColumnDescriptor("metrics"));


        if (!admin.tableExists(tableName.getTableName())) {
            System.out.print("Creating table. ");
            admin.createTable(tableName);
            System.out.println(" Done.");
        }
    }
}
