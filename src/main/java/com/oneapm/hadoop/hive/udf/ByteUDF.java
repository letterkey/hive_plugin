package com.oneapm.hadoop.hive.udf;

import org.apache.hadoop.hive.ql.exec.UDF;

import java.io.UnsupportedEncodingException;

/**
 * Created by YMY on 16-1-21.
 */
public class ByteUDF extends UDF
{
	public int evaluate(String data, String code)
	{
		int len = 0;
		try {
			len = data.getBytes(code).length;
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		} finally {
			return len;
		}
	}
}