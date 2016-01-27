package com.oneapm.hadoop.hive.udf;

import com.oneapm.hadoop.hive.util.FeatureUtil;
import java.util.Map;
import org.apache.hadoop.hive.ql.exec.UDF;
/**
 * Created by YMY on 16-1-21.
 */
public class PropertyUDF extends UDF
{
	public String evaluate(String prop, String key)
	{
		return (String)FeatureUtil.toMap(prop).get(key);
	}
}