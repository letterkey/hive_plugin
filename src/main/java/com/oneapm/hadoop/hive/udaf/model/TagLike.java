package com.oneapm.hadoop.hive.udaf.model;

import com.alibaba.fastjson.JSON;

import java.io.Serializable;

/**
 * Created by peter on 2016/3/16.
 */
public class TagLike implements Serializable, Comparable<TagLike>{

    private String tagName;
    private double weight;

    public TagLike() {
    }

    public TagLike(String tagName, double weight) {
        this.tagName = tagName;
        this.weight = weight;
    }

    public String getTagName() {
        return tagName;
    }

    public void setTagName(String tagName) {
        this.tagName = tagName;
    }

    public double getWeight() {
        return weight;
    }

    public void setWeight(double weight) {
        this.weight = weight;
    }

    @Override
    public String toString() {
        return JSON.toJSONString(this);
    }

    /**
     * 逆序排列
     *
     * @param o
     * @return
     */
    @Override
    public int compareTo(TagLike o) {
        if (o == null) {
            return 0;
        }
        if (this.weight < o.weight) {
            return 1;
        } else if (this.weight > o.weight) {
            return -1;
        }
        return 0;
    }
}
