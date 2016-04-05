package com.oneapm.hadoop.hive.udf;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * 求array的sum值
 * Created by YMY on 16/03/28.
 */
@Description(name = "string_end_of", value = "_FUNC_(str,tarArrayStr) -  "
		, extended = "Example:\n"
		+ "  > SELECT _FUNC_('2016030302','02,01') FROM src LIMIT 1;\n"
		+ "  6.3")
public class StringEndOfUDF extends UDF {

	public boolean evaluate(Object[] data){
		boolean re = false;
		try {
			if(StringUtils.isNotEmpty((String)data[1])) {
				String[] tarArr = data[1].toString().split(",");
				for(String item : tarArr) {
					if (data[0].toString().endsWith(item)) {
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
