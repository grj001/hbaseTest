package com.zhiyou.rowkey;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.util.Bytes;

public class PersonInfo {

	// rowKey 定义
	// 长字节数组, 身份证号,姓名
	// 格式: idCard(18bytes)+name(30bytes)
	public byte[] getRowKey(String idCard, String name) {
		// 验证
		byte[] idCardBytes = Bytes.toBytes(idCard);
		if (idCardBytes.length != 18) {
			System.out.println("身份证位数不对!");
			return null;
		}
		byte[] nameBytes = Bytes.toBytes(name);
		ByteBuffer result = ByteBuffer.allocate(48);
		result.put(idCardBytes);
		result.put(nameBytes);

		System.out.println(result.position());

		byte blank = 0x1F;
		while (result.position() < 48) {
			result.put(blank);
		}

		return result.array();

	}

	public Put generatePersonInfo() {
		String idCard = "411111111111111111";
		String name = "张三";
		Put put = new Put(getRowKey(idCard, name));

		put.addColumn("i".getBytes(), "gender".getBytes(), "男".getBytes());
		put.addColumn("i".getBytes(), "age".getBytes(), "18".getBytes());

		return put;
	}

	

	
	
	
	
	
	public void findByName(Table person, String name) throws IOException {
		Scan scan = new Scan();

		RowFilter filter = 
				new RowFilter(
						CompareFilter.CompareOp.EQUAL
						, new RegexStringComparator(name));

		scan.setFilter(filter);

		ResultScanner rs = person.getScanner(scan);
		Result result =new Result();
		while((result = rs.next()) != null){
			
			byte[] row = result.getRow();
			String idCard = Bytes.toString(row, 0, 18);
			String username = Bytes.toString(row, 18, 30);
			String age = Bytes.toString(result.getValue(Bytes.toBytes("i"), Bytes.toBytes("age")));
			String gender = Bytes.toString(result.getValue(Bytes.toBytes("i"), Bytes.toBytes("gender")));
			
			
			System.out.println("row"+row+"idCard"+idCard+"username"+username+"age"+age+"gender"+gender);
		}

	}
	
	
	public static void main(String[] args) throws IOException {
		PersonInfo personInfo = new PersonInfo();
		// byte[] result = personInfo.getRowKey("411111111111111111", "张三");
		// for(int i=0;i<result.length;i++){
		// System.out.print(result[i]+" ");
		// }

		Connection connection = ConnectionFactory.createConnection(HBaseConfiguration.create());
		// 创建表 create 'bd14:person', 'i'
		Table person = connection.getTable(TableName.valueOf("bd14:person"));
//		person.put(personInfo.generatePersonInfo());
		personInfo.findByName(person, "张三");
	}
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	

}
