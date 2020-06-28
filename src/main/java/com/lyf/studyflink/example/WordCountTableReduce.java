package com.lyf.studyflink.example;

import com.lyf.studyflink.common.CommonUtil;
import com.lyf.studyflink.vo.WC;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.operators.ReduceOperator;
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
public class WordCountTableReduce {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.createCollectionsEnvironment();

        DataSet<WC> input = env.fromElements(
                new WC("120.09", "1"),
                new WC("120.09", "1"),
                new WC("110.09", "2"),
                new WC("100.09", "3"),
                new WC("140.09", "1")
        );
        //通过KeySelector  可以通过两个字段进行分组
        DataSet<WC> pfId = input
                .groupBy(new SelectWord())
                .reduce(new WordCounter());
        pfId.print();

        //通过数据元字段分组
//        DataSet<WC> pfId = input
//                .groupBy("pfId")
//                .reduce(new WordCounter());
//
//        pfId.print();
//        input.print();
    }
    // ReduceFunction that sums Integer attributes of a POJO
    static class WordCounter implements ReduceFunction<WC> {

        @Override
        public WC reduce(WC in1, WC in2) {
            return new WC(in1.getWord() + in2.getWord(), in1.pfId);
        }
    }


    static class SelectWord implements KeySelector<WC, String> {
        @Override
        public String getKey(WC w) {
            return w.getPfId()+w.getWord();
        }
    }

}
