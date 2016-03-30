package com.oneapm.hadoop.hive.udaf.model;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.annotation.JSONField;
import org.codehaus.jackson.annotate.JsonIgnore;

import java.util.List;


/**
 * Created by peter on 2016/3/16.
 */
public class UserTagLike {

    @JSONField(name="_id")
    private String id;
    private List<TagLike> tagLikes;
    private String createTime;

    @JsonIgnore
    private int topN;// 保留topn 个 元素 如果为0 则默认全部保留

    public UserTagLike() {
    }

    public UserTagLike(String id, List<TagLike> tagLikes) {
        this.id = id;
        this.tagLikes = tagLikes;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public List<TagLike> getTagLikes() {
        return tagLikes;
    }

    public void setTagLikes(List<TagLike> tagLikes) {
        this.tagLikes = tagLikes;
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

    @Override
    public String toString() {
        return JSON.toJSONString(this);
    }
}
