package com.oneapm.hadoop.hive.udf;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.Text;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by apple on 15/9/17.
 */
@Description(name = "json_array", value = "_FUNC_(str) - convert json array string to string array "
		, extended = "Example:\n"
		+ "  > SELECT _FUNC_('[1,2,3]') FROM src LIMIT 1;\n"
		+ "  [\"1\", \"2\", \"3\"]")
public class JsonArrayConvertor extends GenericUDF {

	private transient ObjectInspectorConverters.Converter[] converters;

	@Override
	public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
		if (arguments.length != 1) {
			throw new UDFArgumentLengthException(
					"The function json_array(s) takes exactly 1 arguments.");
		}

		converters = new ObjectInspectorConverters.Converter[arguments.length];
		for (int i = 0; i < arguments.length; i++) {
			converters[i] = ObjectInspectorConverters.getConverter(arguments[i],
					PrimitiveObjectInspectorFactory.writableStringObjectInspector);
		}

		return ObjectInspectorFactory
				.getStandardListObjectInspector(PrimitiveObjectInspectorFactory
						.writableStringObjectInspector);
	}

	@Override
	public Object evaluate(DeferredObject[] arguments) throws HiveException {
		if (arguments[0].get() == null) {
			return null;
		}
		Text s = (Text) converters[0].convert(arguments[0].get());

		ArrayList<Text> result = new ArrayList<Text>();

		JSONArray objects = JSON.parseArray(s.toString());
		if (objects.isEmpty()) {
			return null;
		}
		List<Text> array = new ArrayList();
		for (Object object : objects) {
			array.add(new Text(object.toString()));

		}
		return array;
	}

	@Override
	public String getDisplayString(String[] children) {
		assert (children.length == 1);
		return "json_array(" + children[0] + ")";
	}
}
