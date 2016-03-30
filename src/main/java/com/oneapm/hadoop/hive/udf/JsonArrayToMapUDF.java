package com.oneapm.hadoop.hive.udf;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.hive.ql.exec.UDF;


import java.util.HashMap;
import java.util.Map;

/**
 * 求array的sum值
 * Created by YMY on 16/03/28.
 */
//@Description(name = "jsonarray_map", value = "_FUNC_(String[]) - json array element ")
public class JsonArrayToMapUDF extends UDF {

	public String evaluate(String jsondata){
		System.out.println(jsondata);
		JSONArray objects = JSON.parseArray(jsondata);
		if (objects.isEmpty()) {
			return null;
		}
		Map map = new HashMap();
		for (Object object : objects) {
			System.out.println(object.toString());
			JSONObject obj = JSONObject.parseObject(object.toString());
			for(String key : obj.keySet()) {
				map.put(key,obj.get(key));
			}
		}
		return "test";
	}
}
