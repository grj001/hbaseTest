package com.zhiyou.hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

public class HbaseScan {

	public static Configuration configuration = 
			HBaseConfiguration.create();
	
	public Connection connection;


	public Table bd14Test;
	public Admin admin;
	
	public HbaseScan() throws IOException{

		connection = ConnectionFactory
				.createConnection();
		bd14Test = 
				connection.getTable(
						TableName.valueOf("bd14:fromjava"));
		admin = connection.getAdmin();
	}
	
	//scan
	public void scanData() throws IOException{
		
		Scan scan = new Scan();
		
		ResultScanner rs = bd14Test.getScanner(scan);
		Result result = rs.next();
		while(result != null){
			CellScanner cs = result.cellScanner();
			System.out.println("rowkey:\t"+Bytes.toString(result.getRow()));
			
			while(cs.advance()){
				Cell cell = cs.current();
				String family = Bytes.toString(CellUtil.cloneFamily(cell));
				String qualify = Bytes.toString(CellUtil.cloneQualifier(cell));
				String value = Bytes.toString(CellUtil.cloneValue(cell));
				
				System.out.println("family:\t"+family);
				System.out.println("qualify:\t"+qualify);
				System.out.println("value:\t"+value);
				System.out.println();
			}
			
			result = rs.next();
		}
	}
	
	
	public static void main(String[] args) throws IOException {
		HbaseScan hs = new HbaseScan();
		hs.scanData();
	}
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
}
