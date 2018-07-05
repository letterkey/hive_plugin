package com.oneapm.hadoop.hive.udaf;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;

import java.util.*;

/**
 * 适合：滴滴从客服hive中提取有顺序的特征排列
 * Created by YMY on 17/10/10.
 */
public class DIDI_Take_Feature_UDAF extends UDAF {
	private static final Log log = LogFactory.getLog(DIDI_Take_Feature_UDAF.class.getName());
	public static class GroupConcatEvaluate implements UDAFEvaluator {
		// 将一组数据粗放到data
		Map<String,String> data ;

		public GroupConcatEvaluate() {
			super();
			init();
		}

		@Override
		public void init() {
			// treemap根据key自动排序，默认：ASC
			log.info("初始化data：TreeMap");
			data = new TreeMap<String, String>(new Comparator<String>() {
			@Override
			public int compare(String o1, String o2) {
				String[] os1 = o1.split("_");
				String[] os2 = o2.split("_");
//                if(Integer.valueOf(os1[0]) == )
				int mark = 0;
				if(Integer.valueOf(os1[0]) - Integer.valueOf(os2[0]) == 0){
//                    System.out.println("1=");
					if(Integer.valueOf(os1[1]) - Integer.valueOf(os2[1]) == 0 ){
						mark = os1[2].compareTo(os2[2]);
					}else{
						mark = Integer.valueOf(os1[1]) - Integer.valueOf(os2[1]);
					}
				}else{
					mark = Integer.valueOf(os1[0]) - Integer.valueOf(os2[0]);
				}
				return mark;
			}
        });
		}

		/**
		 * 对groupby的每条记录进行操作
		 * @param ite
		 * @return
		 */
		public boolean iterate(String ite,String valid) {
			boolean flag = true;
			if(ite != null && !"".equals(ite)){
				String[] its = ite.split(",");
				for(String it : its){
					String[] i = it.split(":");
					if(valid.equals("k"))
						data.put(i[0],i[0]);
					else
						data.put(i[0],i[1]);
				}
			}else{
				flag=false;
			}
			return flag;
		}

		public Map<String,String> terminatePartial() {
			return data == null ? null : data;
		}

		public boolean merge(Map<String,String> other) {
			boolean flag = true;
			if (other != null) {
				data.putAll(other);
			}
			return flag;
		}

		/**
		 * 最终对data数据操作并返回到客户端
		 * @return
		 */
		public String terminate() {
			String result = mkString(data);
			if(result.endsWith(","))
				result = result.substring(0,result.length()-1);
			return result;
		}

		private String mkString(Map<String,String> d){
			// 所有周期
			String[] cycles = "-1,1,7,14,30,90,180,365".split(",");
//			log.info("当前数据长度："+data.size());
			StringBuffer sb = new StringBuffer();
			Map<String,String> tmp = new HashMap<String, String>();
			// 补长
			for(String key : d.keySet()){
//				log.info("当前Key："+key);
				if(!key.endsWith("avg")) {
					String[] splitK = key.split("_");
					for (String c : cycles) {
						String tmpK = splitK[0] + "_" + c + "_" + splitK[2];
						if (!d.containsKey(tmpK)) {
//							log.info("补长数据：" + tmpK);
							tmp.put(tmpK, "0.0");
						}
					}
				}
			}
			d.putAll(tmp);
//			log.info("最终数据长度："+d.keySet().size());
			for(String key : d.keySet()){
				sb.append(d.get(key)).append(",");
			}
			tmp = null;
//			log.info("mkString："+sb.toString());
			return sb.toString();
		}
	}
}
