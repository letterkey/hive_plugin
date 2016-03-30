package com.oneapm.hadoop.hive.udtf;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorConverter;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.Text;

import java.util.ArrayList;
import java.util.List;

/**
 * Json Array的字符串做归一化
 * @author YMY
 * @version V1.0
 */
public class JsonArrayToNormalizationUDTF extends GenericUDTF {
	@Override
	public StructObjectInspector initialize(ObjectInspector[] argOIs)
			throws UDFArgumentException {
		ArrayList fieldNames = new ArrayList();
		ArrayList fieldOIs = new ArrayList();
		fieldNames.add("userId");
		fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
		fieldNames.add("tagName");
		fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
		fieldNames.add("weight");
		fieldOIs.add(PrimitiveObjectInspectorFactory.javaDoubleObjectInspector);
		return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
	}
	@Override
	public void process(Object[] params) throws HiveException {
		if(params.length == 2 && params[0] != null && params[1] != null) {
			String userId = params[0].toString();
			String json = params[1].toString();
			if (StringUtils.isNotEmpty(json)) {
				List<Object[]> tmp = new ArrayList();
				double total = 0;
				JSONArray objects = JSON.parseArray(json);
				if (objects.size() > 0) {
					for (Object object : objects) {
						JSONObject obj = JSONObject.parseObject(object.toString());
						for (String key : obj.keySet()) {
							Object[] ele = new Object[3];
							ele[0] = userId;
							ele[1] = key;
							ele[2] = obj.getDouble(key);
							tmp.add(ele);
							total += obj.getDouble(key);
//						forward(ele);
						}
					}
				}
				for (Object[] obj : tmp)
					forward(new Object[]{obj[0], obj[1], Double.valueOf(obj[2].toString()) / total});
			}
		}
	}
	@Override
	public void close() throws HiveException {
	}
}
