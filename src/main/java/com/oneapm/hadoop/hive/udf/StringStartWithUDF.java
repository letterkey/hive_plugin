package com.oneapm.hadoop.hive.udf;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * 字符串startwith
 * Created by YMY on 16/06/28.
 */
@Description(name = "string_start_with", value = "_FUNC_(str,tarArrayStr) -  "
		, extended = "Example:\n"
		+ "  > SELECT _FUNC_('2016030302','02,01') FROM src LIMIT 1;\n"
		+ "  6.3")
public class StringStartWithUDF extends UDF {

	public boolean evaluate(Object[] data){
		boolean re = false;
		try {
			if(StringUtils.isNotEmpty((String)data[1])) {
				String[] tarArr = data[1].toString().split(",");
				for(String item : tarArr) {
					if (data[0].toString().startsWith(item)) {
						re = true;
						break;
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			return re;
		}
	}
}
