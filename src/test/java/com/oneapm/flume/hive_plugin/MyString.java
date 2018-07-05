package com.oneapm.flume.hive_plugin;

/**
 * Created by didi on 17/10/9.
 */
import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author Waston Xu
 * @date 2011-4-15 上午10:48:21
 */
public class MyString implements Comparable<MyString> {
    private final String string;
    private List<Object> list;

    public MyString(String string) {
        this.string = string;
        trimString2List();
    }


    private void trimString2List() {
        list = new ArrayList<Object>();
        Integer num = 0;
        StringBuffer sb = new StringBuffer();
        for (int i = 0; i < this.string.length(); i++) {
            char c = this.string.charAt(i);
            if (c > 47 && c < 58) {
                if (sb.length() != 0) {
                    list.add(sb);
                    sb = new StringBuffer();
                }
                num = num * 10 + (c - '0');
                continue;
            } else {
                if (num != 0) {
                    list.add(num);
                    num = 0;
                }
                sb.append(c);
                continue;
            }
        }
        if (sb.length() != 0) {
            list.add(sb);
            sb = new StringBuffer();
        } else if (num != 0) {
            list.add(num);
            num = 0;
        }
    }
    /*
     * 在仔细的观看了String的compareTo方法后，本来打算不调用其API进行编程，
     * 但是考虑到出现"a02"和"a2"这样的情况还是使用其API中的方式解决。
     */
    private int compareToLikeString(String s) {
        int len1 = string.length();
        int len2 = s.length();
        int n = Math.min(len1, len1);
        if (n > 0) {
            int k = 0;
            while (k < n) {
                char c1 = string.charAt(k);
                char c2 = s.charAt(k);
                if (c1 != c2)
                    return c1 - c2;
                k++;
            }
        }

        return len1 - len2;
    }

    @Override
    public int compareTo(MyString anotherString) {
        int len1 = list.size();
        int len2 = anotherString.list.size();
        int n = Math.min(len1, len2);

        int mark = 0;
        if (n > 0) {
            int i = 0;
            while (i < n) {
                Object o1 = list.get(i);
                Object o2 = anotherString.list.get(i);
                if (o1 instanceof Integer && o2 instanceof Integer) {
                    mark = (Integer) o1 - (Integer) o2;
                } else {
                    mark = o1.toString().compareTo(o2.toString());
                }
                if (mark != 0)
                    return mark;
                i++;
            }
        }
        return compareToLikeString(anotherString.string);
        //return string.compareTo(anotherString.string);
    /* 如果这样写会有个弊端就是出现"a02"和"a2"这样的情况，肯定是前面的小*/
        //return string.length() - anotherString.string.length();
    }

    public static void main(String[] args) {
        String s1 = "b9c";
        String s2 = "b09c";
        MyString m1 = new MyString(s1);
        MyString m2 = new MyString(s2);

        System.out.println(m1.compareTo(m2));
    }
}
