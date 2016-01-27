package com.oneapm.hadoop.hive.udtf;

import java.util.ArrayList;
import java.util.Arrays;
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
 * Created by YMY on 16-1-21.
 */
public class JsonUDTF extends GenericUDTF {
	public void close() throws HiveException {
	}

	public StructObjectInspector initialize(ObjectInspector[] argOIs)
			throws UDFArgumentException {
		ArrayList fieldNames = new ArrayList();
		ArrayList fieldOIs = new ArrayList();
		fieldNames.add("agentRunId");
		fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
		fieldNames.add("applicationId");
		fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
		fieldNames.add("startTime");
		fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
		fieldNames.add("endTIme");
		fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
		fieldNames.add("userId");
		fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
		fieldNames.add("metricId");
		fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
		fieldNames.add("typeId");
		fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
		fieldNames.add("scopeId");
		fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
		fieldNames.add("type");
		fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
		fieldNames.add("num1");
		fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
		fieldNames.add("num2");
		fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
		fieldNames.add("num3");
		fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
		fieldNames.add("num4");
		fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
		fieldNames.add("num5");
		fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
		fieldNames.add("num6");
		fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);

		return ObjectInspectorFactory.getStandardStructObjectInspector(
				fieldNames, fieldOIs);
	}

	public void process(Object[] parms) throws HiveException {
		String data = parms[0].toString();
		if (StringUtils.isEmpty(data))
			return;
		String[] dt = data.split("\\|");
		if (dt.length < 2)
			return;
		if (!dt[1].toLowerCase().equals("metricdata"))
			return;
		int inT = data.indexOf("{");
		if (inT < 0)
			return;
		data = data.substring(inT);
		String logic = "equal";
		List where = new ArrayList();
		if (parms.length >= 2) {
			where = Arrays.asList(parms[1].toString().toLowerCase().split("-"));
			if (parms.length >= 3) {
				logic = parms[2].toString().trim().toLowerCase();
			}
		}
		JSONObject obj = null;
		try {
			obj = new JSONObject(data);
			JSONArray metricDataArray = obj.getJSONArray("metricData");
			if (metricDataArray.length() > 0)
				for (int i = 0; i < metricDataArray.length(); i++) {
					JSONArray d = metricDataArray.getJSONArray(i);
					if (d.length() == 10) {
						String tranName = d.getString(3).toLowerCase().trim();
						if ((where.size() > 0)
								&& (logic.equals("equal") ? !where
										.contains(tranName) : logic
										.equals("startwith") ? !startWith(
										where, tranName) : logic
										.equals("endwith") ? !endWith(where,
										tranName) : (!logic.equals("like"))
										|| (!like(where, tranName)))) {
							continue;
						}

						Object[] result = new Object[15];
						result[0] = obj.getString("agentRunId");
						result[1] = obj.getString("applicationId");
						result[2] = obj.getString("startTime");
						result[3] = obj.getString("endTIme");
						result[4] = obj.getString("userId");
						result[5] = d.getString(0);
						result[6] = d.getString(1);
						result[7] = d.getString(2);
						result[8] = d.getString(3);
						result[9] = d.getString(4);
						result[10] = d.getString(5);
						result[11] = d.getString(6);
						result[12] = d.getString(7);
						result[13] = d.getString(8);
						result[14] = d.getString(9);
						forward(result);
					}
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