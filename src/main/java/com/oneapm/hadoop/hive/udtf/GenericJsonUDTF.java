package com.oneapm.hadoop.hive.udtf;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.json.JSONArray;
import org.json.JSONObject;

/**
 * 将json数组转换为多行 Created by YMY on 16-1-21.
 */
public class GenericJsonUDTF extends GenericUDTF {
	public void close() throws HiveException {
	}

	public StructObjectInspector initialize(ObjectInspector[] argOIs)
			throws UDFArgumentException {
		ArrayList fieldNames = new ArrayList();
		ArrayList fieldOIs = new ArrayList();
		fieldNames.add("userId");
		fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
		fieldNames.add("catId");
		fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
		fieldNames.add("weight");
		fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);

		return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
	}

	public void process(Object[] parms) throws HiveException {
		String data = parms[0].toString();
		if (StringUtils.isEmpty(data))
			return;
		try {
			JSONObject obj = new JSONObject(data);
			JSONArray dataArray = obj.getJSONArray("catLikes");
			
			if (dataArray.length() > 0)
				for (int i = 0; i < dataArray.length(); i++) {
					JSONObject d = dataArray.getJSONObject(i);
					Object[] result = new Object[3];
					result[0] = obj.getString("_id");
					result[1] = d.getString("catId");
					result[2] = d.getString("weight");
					forward(result);
				}
		} catch (Exception jsonE) {
			jsonE.printStackTrace();
		}
	}

	private boolean startWith(List<String> list, String data) {
		if ((list == null) || (list.size() <= 0))
			return false;
		for (String s : list) {
			if (data.startsWith(s))
				return true;
		}
		return false;
	}

	private boolean endWith(List<String> list, String data) {
		if ((list == null) || (list.size() <= 0))
			return false;
		for (String s : list) {
			if (data.endsWith(s))
				return true;
		}
		return false;
	}

	private boolean like(List<String> list, String data) {
		if ((list == null) || (list.size() <= 0))
			return false;
		for (String s : list) {
			if (data.indexOf(s) >= 0)
				return true;
		}
		return false;
	}
}