package com.oneapm.hadoop.hive.udf;

//import com.yht.userAgent.UserAgentParser;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * Created by YMY on 17-04-10.
 */
@Description(name = "ua", value = "_FUNC_(ua_string,k) -  "
		, extended = "Example:\n"
		+ "  > SELECT _FUNC_('Mobile/14D27 MicroMessenger/6.5.6 NetType/WIFI Language/zh_CN','micromessenger') FROM src LIMIT 1;\n"
		+ "  6.3")
public class UserAgentUDF extends UDF{
	public String evaluate(String UAString, String k){
		String v= "";
//		try {
//			UserAgentParser userAgentParser = new UserAgentParser();
//
//			if("os".equals(k))
//				v = userAgentParser.OS(UAString);
//			else if("platform".equals(k))
//				v = userAgentParser.platform(UAString);
//			else if("browser".equals(k))
//				v = userAgentParser.browser(UAString);
//			else
//				v = userAgentParser.browserVersion(UAString, k);
//
//		} catch (Exception e) {
//			e.printStackTrace();
//		} finally {
			return v;
		}
	}
