package com.oneapm.hadoop.hive.udf;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * 字符串中存在某个字符判断
 * Created by YMY on 16/06/21.
 */
@Description(name = "string_contains", value = "_FUNC_(str,str) -  "
		, extended = "Example:\n"
		+ "  > SELECT _FUNC_('2016030302','02') FROM src LIMIT 1;\n"
		+ "  6.3")
public class StringContainsUDF extends UDF {

	public boolean evaluate(String src,String sub){
		boolean re = false;
		try {
			if(StringUtils.isNotEmpty(src) && StringUtils.isNotEmpty(sub)) {
				re = src.toLowerCase().contains(sub.toLowerCase());
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			return re;
		}
	}


}
