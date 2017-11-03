package com.zhiyou.rowkey;

import java.util.Date;

public class Test {
	public static void main(String[] args) {
		
		Date date = new Date();
		long dateLong = date.getTime();
		long resultLong = Long.MAX_VALUE - dateLong;
		
		System.out.println(resultLong);
		
	}
}
