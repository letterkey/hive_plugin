package com.oneapm.hadoop.hive.udaf.model;

import com.alibaba.fastjson.JSON;

import java.io.Serializable;

/**
 * Created by apple on 15/9/14.
 */
public class Data implements Serializable, Comparable<Data> {
	private String dataId;
	private Integer callCount;
	private String dataType;
	public Data() {
	}

	public Data(String dataId, int callCount, String dataType) {
		this.dataId = dataId;
		this.callCount = callCount;
		this.dataType = dataType;
	}

	public String getDataId() {
		return dataId;
	}

	public void setDataId(String dataId) {
		this.dataId = dataId;
	}

	public Integer getCallCount() {
		return callCount;
	}

	public void setCallCount(Integer callCount) {
		this.callCount = callCount;
	}

	public String getDataType() {
		return dataType;
	}

	public void setDataType(String dataType) {
		this.dataType = dataType;
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
	public int compareTo(Data o) {
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
