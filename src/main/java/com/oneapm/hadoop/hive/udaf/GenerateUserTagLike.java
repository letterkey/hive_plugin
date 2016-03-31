package com.oneapm.hadoop.hive.udaf;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.oneapm.hadoop.hive.udaf.model.TagLike;
import com.oneapm.hadoop.hive.udaf.model.UserTagLike;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * 用户标签画像 UDAF
 * Created by YMY on 16/03/17.
 */
public class GenerateUserTagLike extends UDAF {
	private static final Logger logger = LoggerFactory.getLogger(GenerateUserTagLike.class);
	public static class TagLikeEvaluate implements UDAFEvaluator {

		UserTagLike userTagLike;
		public TagLikeEvaluate() {
			super();
			userTagLike = new UserTagLike();
			init();
		}

		@Override
		public void init() {
			userTagLike.setTagLikes(new ArrayList<TagLike>());
		}

		/**
		 *
		 * @param uid
		 * @param tagId
		 * @param weight
         * @param topN
         * @return
         */
		public boolean iterate(String uid, String tagId, Double weight,int topN) {
			if (StringUtils.isNotEmpty(uid ) && StringUtils.isNotEmpty(tagId ) && weight != null) {
					userTagLike.setId(uid);
					userTagLike.setTopN(topN);
					userTagLike.getTagLikes().add(new TagLike(tagId,weight));
			}
			return true;
		}

		public UserTagLike terminatePartial() {
			return userTagLike.getId() == null ? null : userTagLike;
		}

		public boolean merge(UserTagLike other) {
			if (other != null) {
				this.userTagLike.setId(other.getId());
				this.userTagLike.getTagLikes().addAll(other.getTagLikes());
				this.userTagLike.setTopN(other.getTopN());
			}
			return true;
		}

		public String terminate() {
			if (userTagLike != null &&
					userTagLike.getId() != null
					&& !userTagLike.getTagLikes().isEmpty()) {
				userTagLike.setCreateTime(String.valueOf(System.currentTimeMillis()));
				Collections.sort(userTagLike.getTagLikes());
				int topN = userTagLike.getTopN()>0 ? userTagLike.getTopN() : 0;
				if (userTagLike.getTagLikes().size() > topN) {
					List<TagLike> tagLikes = userTagLike.getTagLikes();
					if(topN > 0)
						tagLikes = userTagLike.getTagLikes().subList(0, topN);
					normalization(tagLikes);
					userTagLike.setTagLikes(tagLikes);
				}
			}
			//fastjson解析后避免出现下列符号：{"$ref":"$.tagLikes[5]"}  详情参阅SerializerFeature.DisableCircularReferenceDetect 参数
			return userTagLike == null && userTagLike.getId() != null ? null : JSON.toJSONString(userTagLike, SerializerFeature.DisableCircularReferenceDetect);
		}

		private void normalization(List<TagLike> tagLikes) {
			double totalScore = 0.0;
			for (TagLike like : tagLikes) {
				totalScore += like.getWeight();
			}

			for (TagLike like : tagLikes) {
				like.setWeight(like.getWeight() / totalScore);
			}
		}

	}
}
