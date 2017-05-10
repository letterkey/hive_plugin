package com.oneapm.hadoop.hive.udtf;

import java.util.ArrayList;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

/**
 * 将字符串joson数组格式化为jsonarray
 * Created by YMY on 16-8-12.
 */
@Description(name = "jsonarray_udtf", value = "_FUNC_(jsonarrayString) - json array element ")
public class JsonArrayUDTF extends GenericUDTF {
	public void close() throws HiveException {
	}

	public StructObjectInspector initialize(ObjectInspector[] argOIs)
			throws UDFArgumentException {
		if(argOIs.length < 1)
			throw new UDFArgumentException("this jsonarray_udtf(jsonarray str) is one args");
		ArrayList fieldNames = new ArrayList();
		ArrayList fieldOIs = new ArrayList();

		fieldNames.add("jsonobject");
		fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);

		return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
	}

	/**
	 * 将字符串joson数组格式化为jsonarray
	 * @param parms
	 * @throws HiveException
     */
	public void process(Object[] parms) throws HiveException {
		String data = parms[0].toString();
		if (StringUtils.isEmpty(data))
			return;
		try {
			JSONArray array = JSON.parseArray(data);
			for(int i =0; i<array.size(); i ++) {
				Object[] ele = new Object[1];
				if(parms.length > 1) {
					for(int k = 1; k <= parms.length-1 ; k ++) {
						array.getJSONObject(i).put("c"+k, parms[k].toString());
					}
				}
				ele[0] = array.getJSONObject(i).toString();
				forward(ele);
			}
		} catch (Exception jsonE) {
			jsonE.printStackTrace();
		}
	}

}