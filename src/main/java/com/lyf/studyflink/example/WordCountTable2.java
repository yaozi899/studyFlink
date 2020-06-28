package com.lyf.studyflink.example;

import com.lyf.studyflink.common.CommonUtil;
import com.lyf.studyflink.vo.WC;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @description: WordCountTable
 * @author: lyf
 * @create: 2020-06-26 16:13
 */
@Slf4j
public class WordCountTable2 {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.createCollectionsEnvironment();

        //解决方式三匿名内部类
//        env.fromElements(1, 2, 3).map(new MapFunction<Integer, Tuple2<Integer, Integer>>() {
//            @Override
//            public Tuple2<Integer, Integer> map(Integer i) {
//                return Tuple2.of(i, i);
//            }
//        }).print();
        //解决方法二 类
//        env.fromElements(1, 2, 3)
//                .map(new MyTuple2Mapper())
//                .print();
        //解决方法一 returns
//        env.fromElements(1, 2, 3)
//                .map(i -> Tuple2.of(i, i))
//                .returns(Types.TUPLE(Types.INT, Types.INT))
//                .print();
        //报错
//        env.fromElements(1, 2, 3)
//                .map(i -> Tuple2.of(i, i))    // no information about fields of Tuple2
//                .print();

        //初始
//        env.fromElements(1, 2, 3)
//                // returns the squared i
//                .map(i -> i*i)
//                .print();
    }


     static class MyTuple2Mapper implements MapFunction<Integer, Tuple2<Integer, Integer>> {
        @Override
        public Tuple2<Integer, Integer> map(Integer i) {
            return Tuple2.of(i, i);
        }
    }
}
