package com.lyf.studyflink.common;

import org.apache.flink.api.java.tuple.Tuple2;

import java.util.List;

/**
 * @description: commonUtil
 * @author: lyf
 * @create: 2020-06-26 16:47
 */
public class CommonUtil {

    /**
     * 计算结果转为字符串
     *
     * @param returnValue
     * @return
     */
    public static String toString(List<Tuple2<String, String>> returnValue) throws Exception {
        StringBuilder rs = new StringBuilder();

        for (Tuple2<String, String> t : returnValue) {
            rs.append(t.f0).append(":").append(t.f1).append(";");
        }
        return rs.toString();
    }
}
