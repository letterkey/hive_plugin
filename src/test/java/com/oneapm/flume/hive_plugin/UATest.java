package com.oneapm.flume.hive_plugin;


import java.io.IOException;

import com.yht.userAgent.UserAgentParser;
import org.junit.Before;
import org.junit.Test;

/**
 * Created by yinmuyang on 16/7/21.
 */
public class UATest {
    private UserAgentParser userAgentParser;

    @Before
    public void before(){
        userAgentParser = new UserAgentParser();
    }

    @Test
    public void testOS_iphone()
    {
//		String userAgentString = "Mozilla/5.0 (iPhone; CPU iPhone OS 10_2_1 like Mac OS X) AppleWebKit/602.4.6 (KHTML, like Gecko) Mobile/14D27 MicroMessenger/6.5.6 NetType/WIFI Language/zh_CN";
//		String userAgentString = "Mozilla/5.0 (iPhone; CPU iPhone OS 9_1 like Mac OS X) AppleWebKit/601.1.46 (KHTML, like Gecko) Version/9.0 Mobile/13B143 Safari/601.1";
        String userAgentString = "FTAPP/3.6.1 Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/48.0.2564.23 Mobile Safari/537.36";
//        String userAgentString = "Mozilla/5.0 (iPhone; CPU iPhone OS 9_3_2 like Mac OS X) AppleWebKit/601.1.46 (KHTML, like Gecko) Mobile/13F69 MicroMessenger/6.3.22 NetType/4G Language/zh_CN";
        String os = userAgentParser.OS(userAgentString);

        System.out.println(userAgentParser.OS(userAgentString));
        System.out.println(userAgentParser.platform(userAgentString));
        System.out.println(userAgentParser.browser(userAgentString));
        System.out.println(userAgentParser.browserVersion(userAgentString,userAgentParser.browser(userAgentString)));
        System.out.println(userAgentParser.browserVersion(userAgentString,"micromessenger"));
        System.out.println(userAgentParser.browserVersion(userAgentString,"FTAPP"));
        System.out.println(userAgentParser.engine(userAgentString));
//		assertEquals("iPhone OS 5.0.1", os);
    }

    @Test
    public void regUATest(){
//        String uaString = "Mozilla/5.0 (iPhone; CPU iPhone OS 9_1 like Mac OS X) AppleWebKit/601.1.46 (KHTML, like Gecko) Version/9.0 Mobile/13B143 Safari/601.1";
        String uaString = "Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/48.0.2564.23 Mobile Safari/537.36";

    }
}
