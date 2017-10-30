package com.zhiyou.hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

public class HbaseTest {

	
	public Connection connection;
	//用HBaseconfuguration
	//初始化配置信息时候自动加载当前应用classPath下的hbase-site.xml
	public static Configuration configuration = 
			HBaseConfiguration.create();
	
	
	public HbaseTest() throws IOException{
		//对connect初始化
		connection = ConnectionFactory
				.createConnection();
	}
	
	
	public void createTable(String tableName, String... cf1) 
			throws IOException{
		//获取admin对象
		Admin admin = connection.getAdmin();
		TableName tName = TableName.valueOf(tableName);
		HTableDescriptor hDescriptor = 
				new HTableDescriptor(tName);
		if(admin.tableExists(tName)){
			System.out.println("表"+tName+"已存在");
			return;
		}
		//添加列簇信息
		for(String cf : cf1){
			HColumnDescriptor family = new
					HColumnDescriptor(cf);
			hDescriptor.addFamily(family);
		}
		//创建表
		admin.createTable(hDescriptor);
		System.out.println("表"+tableName+"创建成功");
	}
	
	
	
	
	
	
	
	
	
	public void deleteTable(String tableName) 
			throws IOException{
		//获取admin对象
		Admin admin = connection.getAdmin();
		
		TableName tName = TableName.valueOf(tableName);
		
		HTableDescriptor hDescriptor = 
				new HTableDescriptor(tName);
		
		if(!admin.tableExists(tName)){
			System.out.println("表"+tName+"不存在");
			return;
		}
		
		admin.disableTable(tName);
		//创建表
		admin.deleteTable(tName);
		
		System.out.println("表"+tableName+"删除成功");
	}
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	public void cleanUp() throws IOException{
		connection.close();
	}
	
	public static void main(String[] args) 
			throws IOException{
		HbaseTest hbaseTest = new HbaseTest();
//		hbaseTest.createTable("bd14:fromjava", "i","j");
		hbaseTest.deleteTable("bd14:fromjava");
		hbaseTest.cleanUp();
	}
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
}
