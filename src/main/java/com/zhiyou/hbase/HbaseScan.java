package com.zhiyou.hbase;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

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
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.BinaryPrefixComparator;
import org.apache.hadoop.hbase.filter.ColumnPrefixFilter;
import org.apache.hadoop.hbase.filter.ColumnRangeFilter;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.DependentColumnFilter;
import org.apache.hadoop.hbase.filter.FamilyFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FilterList.Operator;
import org.apache.hadoop.hbase.filter.QualifierFilter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.ValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

public class HbaseScan {
	
	public Table hbaseTable;
	public Admin admin;

	public static Configuration configuration = 
			HBaseConfiguration.create();
	
	public Connection connection;

	
	
	public HbaseScan() throws IOException{

		connection = ConnectionFactory
				.createConnection();
		hbaseTable = 
				connection.getTable(
						TableName.valueOf("grj:"+hbaseTable.toString()));
		admin = connection.getAdmin();
	}
	
	//scan
	public void scanData() throws IOException{
		
		Scan scan = new Scan();
	
		//限制条件
		// scan.addFamily(Bytes.toBytes("i"));
		// scan.addColumn(Bytes.toBytes("i"), Bytes.toBytes("age"));
		// scan.setTimeRange(minStamp, maxStamp);
		
		scan.setStartRow(Bytes.toBytes("rowkey_3"));
		//变成闭区间  +1
		scan.setStopRow(Bytes.toBytes("rowkey_5"+1));

		ResultScanner rs = hbaseTable.getScanner(scan);
		showResult(rs);
	}
	
	
	
	//rowfilter
	//包含row的数据
	public void getByRowFilter() throws IOException{
		Scan scan = new Scan();
		
//		RowFilter rowFilter = 
//				new RowFilter(
//						CompareFilter.CompareOp.EQUAL
//						, new RegexStringComparator("5") 
//						);
//		
		
		
		
		//包含5
		RowFilter ltRowFilter = 
				new RowFilter(
						CompareFilter.CompareOp.LESS_OR_EQUAL
						, new BinaryComparator(Bytes.toBytes("rowkey_5")));
	
		
		
		
		scan.setFilter(ltRowFilter);
		
		ResultScanner rScanner = hbaseTable.getScanner(scan);
		showResult(rScanner);
	}
	
	
	
	public void getByFamilyFilter() throws IOException{
		Filter familyFilter = new FamilyFilter
				(CompareFilter.CompareOp.LESS, new BinaryComparator(Bytes.toBytes("j")));
		
		Scan scan = new Scan(); 
		scan.setFilter(familyFilter);
		
		ResultScanner rScanner = hbaseTable.getScanner(scan);
		showResult(rScanner);
	}
	
	
	//列名称比较器, 只返回能匹配的列
	public void getByQualifyFilter() throws IOException{
		
		Filter qualifyFilter = 
				new QualifierFilter(
						CompareFilter.CompareOp.EQUAL
						, new BinaryPrefixComparator(Bytes.toBytes("phone_")));
		Scan scan = new Scan();
		scan.setFilter(qualifyFilter);
		ResultScanner rs = hbaseTable.getScanner(scan);
		showResult(rs);
	}
	
	
	
	public void getByColumnRangeFilter() throws IOException{
		Filter columnRangeFilter = 
				new ColumnRangeFilter(
						Bytes.toBytes("o_name1"), true
						, Bytes.toBytes("o_name1"), true);
		Scan scan = new Scan();
		scan.addFamily(Bytes.toBytes("j"));
		scan.setFilter(columnRangeFilter);
		ResultScanner rs = hbaseTable.getScanner(scan);
		showResult(rs);
	}
	
	
	//按照姓名查找用户信息
	public void GetBySearchValue() throws IOException{
		Filter valueFilter = 
				new ValueFilter(
						CompareFilter.CompareOp.EQUAL
						, new RegexStringComparator("n_6"));
		Scan scan = new Scan();
		scan.setFilter(valueFilter);
		ResultScanner rs = hbaseTable.getScanner(scan);
		showResult(rs);
		
		
	}
	
	//专有过滤器
	public void GetByDependentColumnFilter() throws IOException{
		Filter dependentColumnFilter = 
				new DependentColumnFilter(
						Bytes.toBytes("rowkey_0")
						, Bytes.toBytes("i:age"));
		Scan scan = new Scan();
		scan.setFilter(dependentColumnFilter);
		ResultScanner rs = hbaseTable.getScanner(scan);
		showResult(rs);
		
		
	}
	
	
	public void getByFilterList() throws IOException{
		
		List<Filter> flist = new ArrayList<Filter>();
		
		Filter filter1 = new RowFilter(
				CompareFilter.CompareOp.EQUAL
				, new RegexStringComparator("n_6"));
		flist.add(filter1);
		
		Filter filter2 = new ColumnPrefixFilter(
				Bytes.toBytes("phone_135")
				);
		flist.add(filter2);
		
		Filter filter = new FilterList(Operator.MUST_PASS_ALL,flist);
		
		Scan scan = new Scan();
		scan.setFilter(filter);
		ResultScanner rs = hbaseTable.getScanner(scan);
		showResult(rs);
	}
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	public void showResult(ResultScanner rs) throws IOException{
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
		hs.getByRowFilter();
//		hs.getByFamilyFilter();
//		hs.getByualifyFilter();
//		hs.scanData();
//		hs.getByColumnRangeFilter();
//		hs.getByualifyFilter();
//		hs.GetBySearchValue();
//		hs.GetByDependentColumnFilter();
	}
	
	@Test
	public void Test01() {
		System.out.println(System.currentTimeMillis());
		System.out.println(new Timestamp(System.currentTimeMillis()));
		System.out.println(new Date(System.currentTimeMillis()));
	}
	
	
}
