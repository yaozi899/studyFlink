package com.lyf.studyflink.example;

import com.lyf.studyflink.vo.WC;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.ProjectOperator;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @description: WordCountTable
 * @author: lyf
 * @create: 2020-06-26 16:13
 */
@Slf4j
public class WordCountTableProjection {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.createCollectionsEnvironment();

        DataSet<Tuple3<String, String, String>> input = env.fromElements(
                new Tuple3("2","120.09", "1"),
                new Tuple3("2","130.09", "1"),
                new Tuple3("2","110.09", "2"),
                new Tuple3("2","100.09", "3"),
                new Tuple3("2","140.09", "1")
        );
        // project 单列去重
        //编译错误  需要墙转类型
        //DataSet<Tuple1<String>> ds2 = input.project(2).distinct(0);
        DataSet<Tuple1<String>> ds2 = input.<Tuple1<String>>project(2).distinct(0);
        ds2.print();

        //这里不可以自定义对象 （增减列）
//        DataSet<Tuple2<String, String>> project = input.project(0, 1);
//        project.print();
    }

  
}
