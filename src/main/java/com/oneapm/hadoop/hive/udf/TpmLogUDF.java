package com.oneapm.hadoop.hive.udf;

import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * Created by YMY on 16-1-21.
 */
public class TpmLogUDF extends UDF
{
	public String evaluate(String prop)
	{
		String result = "";
		String[] dt = prop.split("\\|");
		if (dt.length > 2) {
			String typeName = dt[1];
			if (typeName.toLowerCase().equals("metricdata")) {
				String agentRunId = prop.split(",")[0].split(":")[1];
				result = typeName + "\t" + agentRunId;
			} else if ((typeName.toLowerCase().equals("errordata")) || (typeName.toLowerCase().equals("transdata")) || (typeName.toLowerCase().equals("sqltracedata"))) {
				String agentRunId = dt[2].split(",")[0];
				result = typeName + "\t" + agentRunId;
			}
		}
		return result;
	}
}