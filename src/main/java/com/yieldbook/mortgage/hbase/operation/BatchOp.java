package com.yieldbook.mortgage.hbase.operation;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import com.yieldbook.mortgage.hbase.helper.printValues;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.NavigableMap;
public class BatchOp {
	private List<Row> batch = null;
	private Table table = null;
	protected String tableName;
	
	public BatchOp(String tableName) {
		batch = new ArrayList<Row>();
		this.tableName = tableName;
	}
	
	public void ConnectToTable() throws IOException {
		   
	       Configuration conf = HBaseConfiguration.create();
	       Connection connection = ConnectionFactory.createConnection(conf);
	       table = connection.getTable(TableName.valueOf(this.tableName));

	   }
	
    public List<Row> getBatch() {
		return batch;
	}
    
    public void submitBatch() {
        Object[] results = new Object[batch.size()];

        try{
            table.batch(batch,results);
        }catch (Exception e){
            System.err.println("Error: "+e);}

        for (int i=0; i<results.length; i++){
            Result res = (Result) results[i];
            if(!res.isEmpty()){
                printValues.printAllValues((Result) results[i]);
            }
        }   	
    }
    
    public void insertPuts(Put put) {
    	batch.add(put);
    }
    
    public void insertDeletes(Delete delete) {
    	batch.add(delete);
    }
    

	public static void main(String[] args) throws IOException {

        Configuration conf = HBaseConfiguration.create();
        Connection connection = ConnectionFactory.createConnection(conf);
        Table table = connection.getTable(TableName.valueOf("notifications"));

        Put put =new Put(Bytes.toBytes("2"));
        put.addColumn(Bytes.toBytes("attributes"), Bytes.toBytes("type"), Bytes.toBytes("Comment"));
        put.addColumn(Bytes.toBytes("attributes"), Bytes.toBytes("for_user"), Bytes.toBytes("Swetha"));
        put.addColumn(Bytes.toBytes("metrics"), Bytes.toBytes("open"), Bytes.toBytes("0"));

        Get get =new Get(Bytes.toBytes("2"));
        get.addColumn(Bytes.toBytes("attributes"), Bytes.toBytes("type"));
        get.addColumn(Bytes.toBytes("metrics"), Bytes.toBytes("open"));

        Delete delete =new Delete(Bytes.toBytes("2"));
        delete.addColumn(Bytes.toBytes("attributes"), Bytes.toBytes("type"));
        delete.addColumn(Bytes.toBytes("attributes"), Bytes.toBytes("for_user"));

        List<Row> batch = new ArrayList<Row>();
        batch.add(put);
        batch.add(get);
        batch.add(delete);

        Object[] results = new Object[batch.size()];

        try{
            table.batch(batch,results);
        }catch (Exception e){
            System.err.println("Error: "+e);}

        for (int i=0; i<results.length; i++){
            Result res = (Result) results[i];
            if(!res.isEmpty()){
                printValues.printAllValues((Result) results[i]);
            }
        }
    }


    }




