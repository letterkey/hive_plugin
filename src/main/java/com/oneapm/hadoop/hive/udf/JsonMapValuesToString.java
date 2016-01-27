package com.oneapm.hadoop.hive.udf;

import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;

import com.alibaba.fastjson.JSON;
/**
 * 将jsonmap格式的value转换成以固定字符分割的字符串
* @Title: JsonMapValuesToString.java
* @author YMY
* @date 2015年11月13日 下午3:26:50 
* @version V1.0
 */
public class JsonMapValuesToString extends UDF {
	public String evaluate(String jsonMap, String sep) {
		String vs = "";
		try {
			Map<String,String> o = (Map<String,String>)JSON.parse(jsonMap);
			for(String v : o.values()){
				if(StringUtils.isNotEmpty(v))
					vs =vs + sep +v;
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			return vs.replaceFirst(sep, "");
		}
	}
}
