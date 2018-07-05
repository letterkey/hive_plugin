package com.oneapm.flume.hive_plugin;


import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

/**
 * Created by didi on 17/10/9.
 */
public class TestTreeMap {
    @Test
    public void sortByKeyDesc(){
        TreeMap<String, String> tm=new TreeMap<String, String>(new Comparator<String>() {
            @Override
            public int compare(String o1, String o2) {
                return o2.compareTo(o1);
            }
        });
        tm.put("b", "ccc");
        tm.put("a", "ddd");
        tm.put("c", "bbb");
        tm.put("d", "aaa");
        for (String  key : tm.keySet()) {
            System.out.println("key :"+key+",對應的value："+tm.get(key));
        }
    }

    @Test
    public void sortByKeyDesc2(){
        TreeMap<String, String> tm=new TreeMap<String, String>(new Comparator<String>() {
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
        tm.put("0_-1_avg", "0_-1_avg");
        tm.put("0_-1_sum", "0_-1_sum");
        tm.put("0_14_sum", "0_14_sum");
        tm.put("0_180_sum", "0_180_sum");
        tm.put("0_1_sum", "0_1_sum");
        tm.put("0_30_sum", "0_30_sum");
        tm.put("0_365_sum", "0_365_sum");
        tm.put("0_7_sum", "0_7_sum");
        tm.put("0_90_sum", "0_90_sum");

        tm.put("1_-1_avg", "0_-1_avg");
        tm.put("1_-1_sum", "0_-1_sum");
        tm.put("1_14_sum", "0_14_sum");
        tm.put("1_180_sum", "0_180_sum");
        tm.put("1_1_sum", "0_1_sum");
        tm.put("1_30_sum", "0_30_sum");
        tm.put("1_365_sum", "0_365_sum");
        tm.put("1_7_sum", "0_7_sum");
        tm.put("1_90_sum", "0_90_sum");

        tm.put("2_-1_avg", "0_-1_avg");
        tm.put("2_-1_sum", "0_-1_sum");
        tm.put("2_14_sum", "0_14_sum");
        tm.put("2_180_sum", "0_180_sum");
        tm.put("2_1_sum", "0_1_sum");
        tm.put("2_30_sum", "0_30_sum");
        tm.put("2_365_sum", "0_365_sum");
        tm.put("2_7_sum", "0_7_sum");
        tm.put("2_90_sum", "0_90_sum");

        for (String  key : tm.keySet()) {
            System.out.println("key :"+key+",對應的value："+tm.get(key));
        }
    }


    /**
     * 根据value排序
     */
    @Test
    public   void sortByValueDesc(){
        Map<String, String> tm=new TreeMap<String, String>();
        tm.put("a", "ddd");   tm.put("b", "ccc");
        tm.put("c", "bbb");   tm.put("d", "aaa");
        //这里将map.entrySet()转换成list
        List<Map.Entry<String,String>> list = new ArrayList<Map.Entry<String,String>>(tm.entrySet());
        //然后通过比较器来实现排序
        Collections.sort(list,new Comparator<Map.Entry<String,String>>() {
            //降序排序
            @Override
            public int compare(Entry<String, String> o1,
                               Entry<String, String> o2) {
                return o2.getValue().compareTo(o1.getValue());
            }
        });

        for(Map.Entry<String,String> mapping:list){
            System.out.println(mapping.getKey()+":"+mapping.getValue());
        }
    }

}
