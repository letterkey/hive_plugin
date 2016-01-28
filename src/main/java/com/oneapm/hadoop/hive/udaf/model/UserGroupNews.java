package com.oneapm.hadoop.hive.udaf.model;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.annotation.JSONField;
import org.codehaus.jackson.annotate.JsonIgnore;

import java.io.Serializable;
import java.util.List;

/**
 * Created by apple on 15/9/14.
 * update by YMY 15/11/13
 */
public class UserGroupNews implements Serializable {

	@JSONField(name="_id")
	private String id;
	private List<News> data;
	private String createTime;
	private String group;

	@JsonIgnore
	private int topN;// 保留topn 个 元素 如果为0 则默认全部保留

	public UserGroupNews() {
	}

	public UserGroupNews(String id, List<News> data) {
		this.id = id;
		this.data = data;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public List<News> getData() {
		return data;
	}

	public void setData(List<News> data) {
		this.data = data;
	}

	public String getCreateTime() {
		return createTime;
	}

	public void setCreateTime(String createTime) {
		this.createTime = createTime;
	}

	public int getTopN() {
		return topN;
	}

	public void setTopN(int topN) {
		this.topN = topN;
	}

	public String getGroup() {
		return group;
	}

	public void setGroup(String group) {
		this.group = group;
	}

	@Override
	public String toString() {
		return JSON.toJSONString(this);
	}

}
