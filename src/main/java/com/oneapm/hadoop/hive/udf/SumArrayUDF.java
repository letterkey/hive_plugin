package com.oneapm.hadoop.hive.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * 求array的sum值
 * Created by YMY on 16/03/28.
 */
@Description(name = "sum_array", value = "_FUNC_(double) - sum array element "
		, extended = "Example:\n"
		+ "  > SELECT _FUNC_('[1.0,2.1,3.2]') FROM src LIMIT 1;\n"
		+ "  6.3")
public class SumArrayUDF extends UDF {

	public double evaluate(Object[] data){
		double sum = 0.0;
		try {
			for(Object t : data)
				sum += (Double)t;
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			return sum;
		}
	}


}
