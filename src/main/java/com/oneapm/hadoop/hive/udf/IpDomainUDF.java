package com.oneapm.hadoop.hive.udf;

import java.net.URL;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hadoop.hive.ql.exec.UDF;
/**
 * Created by YMY on 16-1-21.
 */
public class IpDomainUDF extends UDF{
	public String evaluate(String url, String action)
	{
		String domain = "";
		try {
			URL url1 = new URL(url);
			domain = url1.getHost();
		} catch (Exception e) {
			return domain = "";
		}

		if (action.equals("1"))
		{
			String ipReg = "\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}";
			Pattern p = Pattern.compile(ipReg);

			if (!p.matcher(domain).matches())
			{
				String doReg = "[0-9a-zA-Z]+((\\.com)|(\\.cn)|(\\.org)|(\\.net)|(\\.edu)|(\\.com.cn))";
				Pattern pDo = Pattern.compile(doReg);
				Matcher m = pDo.matcher(domain);
				if (m.find()) {
					domain = m.group();
				}
			}
		}

		return domain;
	}

	public static void main(String[] args) {
		IpDomainUDF ipD = new IpDomainUDF();
		String domain = ipD.evaluate("http://127.0.0.1/xxx.html", "1");
		System.out.println(domain);
	}
}
