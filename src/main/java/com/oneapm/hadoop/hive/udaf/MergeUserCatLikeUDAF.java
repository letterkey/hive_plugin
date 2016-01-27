package com.oneapm.hadoop.hive.udaf;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;

import com.alibaba.fastjson.JSON;
import com.oneapm.hadoop.hive.udaf.model.CatLike;
import com.oneapm.hadoop.hive.udaf.model.UserCatLike;

/**
 * Created by YMY on 16/01/13.
 */
public class MergeUserCatLikeUDAF extends UDAF {

	public static class UserCatLikeMerge implements UDAFEvaluator {
		Map<Integer,UserCatLike> catMap;
		public UserCatLikeMerge() {
			super();
			init();
		}

		@Override
		public void init() {
			catMap = new HashMap();
		}

		/**
		 * map阶段调用：hive group后的迭代器
		 * @param uid
		 * @param jsonData
		 * @param seq
         * @param topN
         * @return
         */
		public boolean iterate(String uid, String  jsonData, int seq,int topN) {
			if (uid != null && StringUtils.isNotEmpty(jsonData)) {
				UserCatLike ucl = JSON.parseObject(jsonData,UserCatLike.class);
				ucl.setId(uid);
				ucl.setTopN(topN);
				catMap.put(seq,ucl);
			}
			return true;
		}

		/**
		 * mapper结束要返回的结果，还有combiner结束返回的结果
		 * @return
         */
		public Map<Integer,UserCatLike> terminatePartial() {
			return catMap;
		}

		/**
		 * combiner合并map返回的结果，还有reducer合并mapper或combiner返回的结果
		 * @param other
         * @return
         */
		public boolean merge(Map<Integer,UserCatLike> other) {
			if (other != null) {
				this.catMap.putAll(other);
			}
			return true;
		}

		/**
		 * reducer返回结果，或者是只有mapper，没有reducer时，在mapper端返回结果。
		 * @return
         */
		public String terminate() {
			UserCatLike userCatLike = null;
			if (catMap != null && catMap.size() > 0) {
				Map<Integer,CatLike> tmp = new HashMap();
				// 获得最大的序列
				int maxSeq = Collections.max(catMap.keySet());

				userCatLike = catMap.get(maxSeq);
				int deno = rangeAgg(1,maxSeq);
				for(int seq : catMap.keySet()){
					double factor = seq > 0 ? seq/(deno*1.0) : 1.0;
					if(maxSeq == 1){
						factor=0.8;
					}
					UserCatLike uc = catMap.get(seq);
					if(uc.getCatLikes() != null && uc.getCatLikes().size() > 0 ){
						for(CatLike cl : uc.getCatLikes()){
							CatLike ctmp = tmp.get(cl.getCatId());
							if(ctmp == null) {
								ctmp = new CatLike(cl.getCatId(), cl.getWeight() * factor);
								tmp.put(cl.getCatId(), ctmp);
							}else {
								ctmp.setWeight(ctmp.getWeight() + cl.getWeight() * factor);
							}
						}
					}
				}

				// 计算制定长度的归一化
				userCatLike.setCreateTime(String.valueOf(System.currentTimeMillis()));
				List<CatLike> t = new ArrayList();
				for(CatLike c : tmp.values())
					t.add(c);
//				Arrays.asList((CatLike[])tmp.values().toArray());
				userCatLike.setCatLikes(t);

				Collections.sort(userCatLike.getCatLikes());
				int topN = userCatLike.getTopN()>0 ? userCatLike.getTopN() : 0;
				if (userCatLike.getCatLikes().size() > topN) {
					List<CatLike> catLikes = userCatLike.getCatLikes();
					if(topN > 0)
						catLikes = userCatLike.getCatLikes().subList(0, topN);
					normalization(catLikes);
					userCatLike.setCatLikes(catLikes);
				}
			}
			return userCatLike == null ? null : JSON.toJSONString(userCatLike);
		}

		private void normalization(List<CatLike> catLikes) {
			double totalScore = 0.0;
			for (CatLike catLike : catLikes) {
				totalScore += catLike.getWeight();
			}

			for (CatLike catLike : catLikes) {
				catLike.setWeight(catLike.getWeight() / totalScore);
			}
		}
		private int rangeAgg(int start ,int end){
			int result= 0;
			for(int i = start; i <= end ; i ++)
				result += i;
			return result;
		}
	}
}
