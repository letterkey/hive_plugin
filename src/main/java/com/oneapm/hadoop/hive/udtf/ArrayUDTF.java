package com.oneapm.hadoop.hive.udtf;

import java.util.ArrayList;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;

/**
 * 将数组经udtf转换为多行
 * 
 * @Title: ArrayUDTF.java
 * @author YMY
 * @date 2015年11月17日 下午2:07:48
 * @version V1.0
 */
public class ArrayUDTF extends GenericUDTF {
	@Override
	public StructObjectInspector initialize(ObjectInspector[] argOIs)
			throws UDFArgumentException {
		ArrayList fieldNames = new ArrayList();
		ArrayList fieldOIs = new ArrayList();
		fieldNames.add("catId");
		fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
		fieldNames.add("weight");
		fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
		return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
	}
	@Override
	public void process(Object[] params) throws HiveException {
		String json = String.valueOf(params[0]);
		if(StringUtils.isNotEmpty(json)){
			JSONArray ja = JSON.parseArray(json);
			if(ja.size()>0){
				for(int i = 0; i <= ja.size() -1; i ++){
					Object[] ele = new Object[2];
					ele[0] = ja.getString(i);
					ele[1] = String.valueOf(1.0/ja.size());
					forward(ele);
				}
			}
		}
	}
	@Override
	public void close() throws HiveException {
	}
}
