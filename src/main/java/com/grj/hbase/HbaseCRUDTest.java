package com.grj.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HbaseCRUDTest {

	public static Logger logger = LoggerFactory.getLogger(HbaseCRUDTest.class);

	public static final String tableNameStr = "grj:test";

	/**
	 * 获取hbase的配置信息 初始化配置信息时候自动加载当前应用classPath下的hbase-site.xml
	 */
	public static Configuration configuration = HBaseConfiguration.create();

	public Admin admin;
	public Table table;
	public TableName tableName;
	public Connection connection;

	/**
	 * 初始化connection,admin,tablename,hbasetable
	 */
	public HbaseCRUDTest() throws IOException {
		// 对connect初始化
		connection = ConnectionFactory.createConnection();
		admin = connection.getAdmin();
		tableName = TableName.valueOf(tableNameStr);
		table = connection.getTable(tableName);
	}

	/**
	 * 创建hbase 表
	 */
	public void createTable(String... cf1) throws IOException {
		logger.info("createTable begin......................");
		HTableDescriptor hTableDescriptor;
		HColumnDescriptor hColumnDescriptor;

		// 判断表是否存在
		if (admin.tableExists(tableName)) {
			System.out.println("表" + tableName + "已存在");
			return;
		}

		// 创建 hTableDescriptor
		hTableDescriptor = new HTableDescriptor(tableName);

		// 添加列簇信息
		for (String cf : cf1) {
			hColumnDescriptor = new HColumnDescriptor(cf);
			hTableDescriptor.addFamily(hColumnDescriptor);
		}

		// 使用 admin 创建表
		admin.createTable(hTableDescriptor);
		logger.info("createTable end .......................");
	}

	/**
	 * 新增数据
	 */
	public void putData() throws IOException {
		logger.info("putData begin .......................");
		table = connection.getTable(tableName);

		Random random = new Random();

		List<Put> butPut = new ArrayList<Put>();

		for (int i = 0; i < 10; i++) {
			Put put = new Put(Bytes.toBytes("rowkey_" + i));
			put.addColumn(Bytes.toBytes("i"), Bytes.toBytes("username"), Bytes.toBytes("un_" + i));
			put.addColumn(Bytes.toBytes("i"), Bytes.toBytes("age"), Bytes.toBytes(random.nextInt(50) + 1));
			put.addColumn(Bytes.toBytes("i"), Bytes.toBytes("birthday"), Bytes.toBytes("20170" + i + "01"));
			put.addColumn(Bytes.toBytes("j"), Bytes.toBytes("phone"), Bytes.toBytes("电话_" + i));
			put.addColumn(Bytes.toBytes("j"), Bytes.toBytes("email"), Bytes.toBytes("email_" + i));
			// 单记录put
			// table.put(butPut);
			butPut.add(put);
		}
		table.put(butPut);
		logger.info("putData end .......................");
	}

	/**
	 * 从hbase获取数据
	 */
	public void getData() throws IOException {
		logger.info("getData end .......................");
		// 构建get对象
		List<Get> gets = new ArrayList<Get>();

		for (int i = 0; i < 5; i++) {
			Get get = new Get(Bytes.toBytes("rowkey_" + i));
			gets.add(get);
		}
		Result[] results = table.get(gets);

		// 结果集
		for (Result result : results) {
			/*
			 * NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long,
			 * byte[]>>> maps = result.getMap();
			 * 
			 * //列簇 for(byte[] cf : maps.keySet()){
			 * 
			 * NavigableMap<byte[], NavigableMap<Long, byte[]>>
			 * valueWithColumnQualify = maps.get(cf);
			 * 
			 * //列 for(byte[] columnQualify : valueWithColumnQualify.keySet()){
			 * 
			 * NavigableMap<Long, byte[]> valueWithColumnStamp =
			 * valueWithColumnQualify.get(columnQualify);
			 * 
			 * for(Long ts : valueWithColumnStamp.keySet()){ byte[] value =
			 * valueWithColumnStamp.get(ts); System.out.println(
			 * "rowkey"+Bytes.toString(result.getRow())
			 * +",columnFamily:"+Bytes.toString(cf)
			 * +",columnQualify:"+Bytes.toString(columnQualify)
			 * +",timestamp:"+new Date(ts) +",value"+Bytes.toString(value) ); }
			 * } }
			 */
			// System.out.println("rowkey:"
			// +Bytes.toString(result.getRow())
			// +",columnfamily:i, columnqualiy:username, value:"+
			// Bytes.toString(
			// result.getValue(
			// Bytes.toBytes("i")
			// , Bytes.toBytes("username"))));
			//
			// System.out.println("rowkey:"
			// +Bytes.toString(result.getRow())+
			// ",columnfamily:i, columnqualiy:age, value:"+
			// Bytes.toInt(
			// result.getValue(Bytes.toBytes("i"), Bytes.toBytes("age"))));

			CellScanner cellScanner = result.cellScanner();
			System.out.println("**\t" + cellScanner.toString());
			while (cellScanner.advance()) {
				Cell cell = cellScanner.current();

				String family = Bytes.toString(CellUtil.cloneFamily(cell));

				String quaily = Bytes.toString(CellUtil.cloneQualifier(cell));

				String rowkey = Bytes.toString(CellUtil.cloneRow(cell));

				String value = Bytes.toString(CellUtil.cloneValue(cell));
				System.out.println("family" + family + "quaily" + quaily + "rowkey" + rowkey + "value" + value);
			}
			// System.out.println(result.toString());
		}
		logger.info("getData end .......................");
	}

	/**
	 * delete hbase table
	 */
	public void deleteTable() throws IOException {
		logger.info("deleteTable begin .......................");

		HTableDescriptor hDescriptor;

		hDescriptor = new HTableDescriptor(tableName);

		// 判断表不存在,返回
		if (!admin.tableExists(tableName)) {
			System.out.println("表" + tableName + "不存在");
			return;
		}

		admin.disableTable(tableName);
		// 创建表
		admin.deleteTable(tableName);

		logger.info("deleteTable end .......................");
	}

	public void cleanUp() throws IOException {
		connection.close();
	}

	public static void main(String[] args) throws IOException {
		HbaseCRUDTest hbaseCRUDTest = new HbaseCRUDTest();

		// create_namespace 'grj'
		// hbaseCRUDTest.createTable("i", "j");

		// hbaseCRUDTest.putData();
		// hbaseCRUDTest.getData();
		// hbaseCRUDTest.deleteTable();
		hbaseCRUDTest.cleanUp();
	}

}
