package com.oneapm.flume.hive_plugin;

import java.text.DecimalFormat;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.junit.Test;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;

public class TestCase {
	
	@Test
	public void Test1(){
		Map<String,String> o = (Map<String,String>)JSON.parse(
				"{\"20000207b4e5685b\":\"40\",\"2000025dee4fb485\":\"23\",\"200002d04726f684\":\"23\",\"2fcac318480\":\"31\",\"20000315e0f6d5d9\":\"40\",\"2000025d892f75cc\":\"23\",\"2000026edf245262\":\"40\",\"2000036dc256e7ff\":\"26\"}");
		System.out.println(o.get("20000207b4e5685b"));
	}
	
	@Test
	public void Test2(){
		System.out.println(StringUtils.isNotEmpty(null));
		Map<String,String> o = (Map<String,String>)JSON.parse(
			    "{\"app\":\"0.0.1\",\"createTime\":1447408001553,\"data\":\"[\\\"11\\\",\\\"12\\\",\\\"3\\\"]\",\"deviceId\":\"867323022385007\",\"deviceType\":\"01\",\"topic\":\"topic_personalization_event\",\"userId\":\"71f640cc01bf70803c6baefa46a9bd9d\"}");
		System.out.println(o.get("data"));
	}
	
	@Test
	public void stringToArray(){
		JSONArray o = JSON.parseArray(
				"[8,9,12,7]");
		System.out.println(o.size());
	}
	
	@Test
	public void doubleS(){
		System.out.println(1/3.0);
	}
}
