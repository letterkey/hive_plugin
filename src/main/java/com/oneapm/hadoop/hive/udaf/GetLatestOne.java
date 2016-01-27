package com.oneapm.hadoop.hive.udaf;

import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;

/**
 *  求出一个group中的时间最近的一条记录（可以随后扩展）
* @Title: GetLatestOne.java
* @author YMY
* @date 2015年11月26日 下午2:52:05 
* @version V1.0
 */
public class GetLatestOne extends UDAF {

	public static class GetLatestEvaluate implements UDAFEvaluator {
		String uid; 
		Long latestTime;
		String result ;
		
		public GetLatestEvaluate() {
			super();
			latestTime = 0l;
			result = ""; 
			uid = ""; 
			init();
		}
		@Override
		public void init() {
		}

		public boolean iterate(String userId, Long createTime,String data) {
			if (userId != null && userId != null && data != null && createTime >latestTime ) {
				latestTime = createTime;
				result = data;
				uid = userId;
				latestTime = createTime;
			}
			return true;
		}

		public String terminatePartial() {
			return result == null ? null : uid+"-"+result+"-"+latestTime;
		}

		public boolean merge(String r) {
			this.result = r;
			return true;
		}
		public String terminate() {
			return this.result;
		}
	}
}
