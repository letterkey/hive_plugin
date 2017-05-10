package com.oneapm.flume.hive_plugin;

import eu.bitwalker.useragentutils.UserAgent;
import org.junit.Test;

/**
 * Created by yinmuyang on 17/4/6.
 */
public class UA_TEST {

    @Test
    public void test1(){
        String uaString="FTAPP/3.6.1 Mozilla/5.0 (iPhone; CPU iPhone OS 9_0 like Mac OS X) AppleWebKit/601.1.46 (KHTML, like Gecko) Mobile/13A342";
        UserAgent userAgent = UserAgent.parseUserAgentString(uaString);
        System.out.println(userAgent.getBrowser());
        System.out.println(userAgent.getBrowserVersion());
        System.out.println(userAgent.getOperatingSystem());
        System.out.println(userAgent.toString());
    }
}
