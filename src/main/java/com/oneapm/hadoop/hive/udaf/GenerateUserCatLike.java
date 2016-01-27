package com.oneapm.hadoop.hive.udaf;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;

import com.alibaba.fastjson.JSON;
import com.oneapm.hadoop.hive.udaf.model.CatLike;
import com.oneapm.hadoop.hive.udaf.model.UserCatLike;

/**
 * Created by apple on 15/9/14.
 */
public class GenerateUserCatLike extends UDAF {

	public static class UserCatLikeEvaluate implements UDAFEvaluator {

		UserCatLike userCatLike;

		public UserCatLikeEvaluate() {
			super();
			userCatLike = new UserCatLike();
			init();
		}

		@Override
		public void init() {
			userCatLike.setId(null);
			userCatLike.setCatLikes(new ArrayList<CatLike>());
		}

		/**
		 *
		 * @param uid
		 * @param catId
		 * @param weight
         * @param topN
         * @return
         */
		public boolean iterate(String uid, Integer catId, Double weight,int topN) {
			if (uid != null && catId != null && weight != null) {
				userCatLike.setId(uid);
				userCatLike.setTopN(topN);
				userCatLike.getCatLikes().add(new CatLike(catId, weight));
			}
			return true;
		}

		public UserCatLike terminatePartial() {
			return userCatLike.getId() == null ? null : userCatLike;
		}

		public boolean merge(UserCatLike other) {
			if (other != null) {
				this.userCatLike.setId(other.getId());
				this.userCatLike.setTopN(other.getTopN());
				this.userCatLike.getCatLikes().addAll(other.getCatLikes());
			}
			return true;
		}

		public String terminate() {
			if (userCatLike != null &&
					userCatLike.getId() != null
					&& !userCatLike.getCatLikes().isEmpty()) {
				userCatLike.setCreateTime(String.valueOf(System.currentTimeMillis()));

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
			return userCatLike == null && userCatLike.getId() != null ? null : JSON.toJSONString(userCatLike);
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

	}
}
