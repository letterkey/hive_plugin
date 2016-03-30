package com.oneapm.hadoop.hive.udaf;

import com.alibaba.fastjson.JSON;
import com.oneapm.hadoop.hive.udaf.model.Data;
import com.oneapm.hadoop.hive.udaf.model.UserGroupNews;
import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Created by apple on 15/9/14.
 */
public class UserGroupNewsUDAF extends UDAF {

	public static class UserGroupNewsEvaluate implements UDAFEvaluator {

		UserGroupNews data;

		public UserGroupNewsEvaluate() {
			super();
			data = new UserGroupNews();
			init();
		}

		@Override
		public void init() {
			data.setId(null);
			data.setData(new ArrayList<Data>());
		}

		/**
		 *
		 * @param uid
		 * @param group
		 * @param newsId
		 * @param callCount
         * @param topN
         * @return
         */
		public boolean iterate(String uid,String group, String newsId, int callCount,String dataType,int topN) {
			if (uid != null && newsId != null) {
				data.setId(uid);
				data.setTopN(topN);
				data.setGroup(group);
				data.getData().add(new Data(newsId, callCount,dataType));
			}
			return true;
		}

		public UserGroupNews terminatePartial() {
			return data.getId() == null ? null : data;
		}

		public boolean merge(UserGroupNews other) {
			if (other != null) {
				this.data.setId(other.getId());
				this.data.setTopN(other.getTopN());
				this.data.setGroup(other.getGroup());
				this.data.getData().addAll(other.getData());
			}
			return true;
		}

		public String terminate() {
			if (data != null &&
					data.getId() != null
					&& !data.getData().isEmpty()) {
				data.setCreateTime(String.valueOf(System.currentTimeMillis()));

				Collections.sort(data.getData());
				int topN = data.getTopN()>0 ? data.getTopN() : data.getData().size();
				data.setTopN(topN);
				if (data.getData().size() > topN) {
					List<Data> ns = data.getData();
					if(topN > 0)
						ns = data.getData().subList(0, topN);
					data.setData(ns);
				}
			}
			return data == null && data.getId() != null ? null : JSON.toJSONString(data);
		}
	}
}
