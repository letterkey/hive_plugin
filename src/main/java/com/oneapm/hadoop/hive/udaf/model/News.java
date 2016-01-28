package com.oneapm.hadoop.hive.udaf.model;

import com.alibaba.fastjson.JSON;

import java.io.Serializable;

/**
 * Created by apple on 15/9/14.
 */
public class News implements Serializable, Comparable<News> {
	private String newsId;
	private Integer callCount;

	public News() {
	}

	public News(String newsId, int callCount) {
		this.newsId = newsId;
		this.callCount = callCount;
	}

	public String getNewsId() {
		return newsId;
	}

	public void setNewsId(String newsId) {
		this.newsId = newsId;
	}

	public Integer getCallCount() {
		return callCount;
	}

	public void setCallCount(Integer callCount) {
		this.callCount = callCount;
	}

	@Override
	public String toString() {
		return JSON.toJSONString(this);
	}

	/**
	 * 逆序排列 desc
	 *
	 * @param o
	 * @return
	 */
	@Override
	public int compareTo(News o) {
		if (o == null) {
			return 0;
		}
		if (this.callCount < o.callCount) {
			return 1;
		} else if (this.callCount > o.callCount) {
			return -1;
		}
		return 0;
	}
}
