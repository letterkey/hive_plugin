package com.oneapm.hadoop.hive.util;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

/**
 * Created by YMY on 16-1-21.
 */
public class FeatureUtil {
	public static final String SP = ";";
	public static final String SSP = ":";
	static final String R_SP = "#3A";
	static final String R_SSP = "#3B";

	private static String encode(String val) {
		return StringUtils.replace(StringUtils.replace(val, ";", "#3A"), ":",
				"#3B");
	}

	private static String decode(String val) {
		return StringUtils.replace(StringUtils.replace(val, "#3A", ";"), "#3B",
				":");
	}

	public static final Map<String, String> toMap(String str) {
		Map attrs = new HashMap();
		if (StringUtils.isNotBlank(str)) {
			String[] arr = StringUtils.split(str, ";");
			for (String kv : arr) {
				if (StringUtils.isNotBlank(kv)) {
					String[] ar = kv.split(":");
					if (ar.length == 2) {
						String k = decode(ar[0]);
						String v = decode(ar[1]);
						if ((!StringUtils.isNotBlank(k))
								|| (!StringUtils.isNotBlank(v)))
							continue;
						attrs.put(k, v);
					}
				}
			}
		}
		return attrs;
	}

	public static final String toString(Map<String, String> attrs) {
		StringBuilder sb = new StringBuilder();
		if ((attrs != null) && (!attrs.isEmpty())) {
			sb.append(";");
			for (String key : attrs.keySet()) {
				String val = attrs.get(key);
				if (!StringUtils.isNotEmpty(val))
					continue;
				sb.append(encode(key)).append(":").append(encode(val)).append(";");
			}
		}
		return sb.toString();
	}
}