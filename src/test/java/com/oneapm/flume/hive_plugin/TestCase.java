package com.oneapm.flume.hive_plugin;

import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.Map;

import com.alibaba.fastjson.JSONObject;
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
	//["{\"阿杰\":254.18813760723296}","{\"爸爸\":124.01170081383111}","{\"宝宝\":69.11225944625576}","{\"父母\":67.96744799486767}","{\"孩子\":53.443821294871626}","{\"游戏\":52.93198900792951}","{\"妈妈\":52.59076748330145}","{\"广州\":46.073973611627814}"]

	@Test
	public void doubleS(){
		String s = "[{\"阿杰\":254.18813760723296},{\"爸爸\":124.01170081383111},{\"宝宝\":69.11225944625576},{\"父母\":67.96744799486767},{\"孩子\":53.443821294871626},{\"游戏\":52.93198900792951},{\"妈妈\":52.59076748330145},{\"广州\":46.073973611627814}]";
		JSONArray objects = JSON.parseArray(s.toString());

		Map map = new HashMap();
		for (Object object : objects) {
			System.out.println(object.toString());

			JSONObject obj = JSONObject.parseObject(object.toString());
			for(String key : obj.keySet()) {
				System.out.println(key +":"+obj.getString(key));
				map.put(key,obj.get(key));
			}

		}
	}
}
