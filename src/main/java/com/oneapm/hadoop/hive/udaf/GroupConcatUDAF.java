package com.oneapm.hadoop.hive.udaf;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;

/**
 * mysql group_concat
 * Created by YMY on 17/09/30.
 */
@Description(name = "group_concat", value = "_FUNC_(str) -  "
		, extended = "Example:\n"
		+ "  > SELECT _FUNC_(colum) FROM src GROUP BY key;\n"
		+ "  r1c1,r2c1,r3c1..")
public class GroupConcatUDAF extends UDAF {
	private static final Log log = LogFactory.getLog(GroupConcatUDAF.class.getName());
	public static class GroupConcatEvaluate implements UDAFEvaluator {

		// 将一组数据粗放到data
		StringBuffer data ;
		String separator = " ";
		public GroupConcatEvaluate() {
			super();
//			log.info("创建udaf 对象");
			data = new StringBuffer();
			init();
		}

		@Override
		public void init() {
//			log.info("init my udaf ");
//			data = new StringBuffer();
//			log.info("data初始值:"+data);
		}

		/**
		 * 对groupby的每条记录进行操作
		 * @param ite
		 * @return
		 */
		public boolean iterate(String ite,String sep) {
//			log.info("接收到参数："+ite);
//			if (data == null)
//				data = new StringBuffer();
			data.append(ite).append(sep);
//			log.info("累加结果："+data.toString());
			return true;
		}

		public String terminatePartial() {
			return data.toString();
		}

		public boolean merge(String other) {
			if (StringUtils.isNotEmpty(other))
				this.data.append(other).append(separator);
			return true;
		}
		/**
		 * 最终对data数据操作并返回到客户端
		 * @return
		 */
		public String terminate() {
//			log.info("最终返回："+data.toString());
			return data.toString();
		}
	}
}
