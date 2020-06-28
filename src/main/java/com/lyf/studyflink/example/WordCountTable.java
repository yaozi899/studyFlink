package com.lyf.studyflink.example;

import com.lyf.studyflink.common.CommonUtil;
import com.lyf.studyflink.vo.WC;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
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
public class WordCountTable {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.createCollectionsEnvironment();

        DataSet<WC> input = env.fromElements(
                new WC("120.09", "1"),
                new WC("130.09", "1"),
                new WC("110.09", "2"),
                new WC("100.09", "3"),
                new WC("140.09", "1")
        );

        DataSet<Tuple2<Double, String>> map = input.map(new MyMapFunction());

        List<Tuple2<String, String>> collect = map.groupBy(1).reduceGroup(new MyGroupReduceFunction()).collect();

        System.out.println(CommonUtil.toString(collect));

//        input.print();
    }

    static class MyMapFunction implements MapFunction<WC, Tuple2<Double, String>> {

        @Override
        public Tuple2<Double, String> map(WC wc) {
            return new Tuple2<>(Double.parseDouble(wc.getWord()),
                    wc.getPfId());
        }
    }


    static class  MyGroupReduceFunction implements GroupReduceFunction<Tuple2<Double, String>,Tuple2<String, String>> {
        @Override
        public void reduce(Iterable<Tuple2<Double, String>> iterable, Collector<Tuple2<String, String>> collector) throws Exception {
            List<Double> collect = new ArrayList<Double>();
            AtomicReference<String> key = new AtomicReference<String>("");

            iterable.forEach(wc -> {
                collect.add(Double.parseDouble(wc.getField(0).toString()));
                key.set(wc.getField(1));
            });

            Double aDouble = collect.stream().mapToDouble(Double::doubleValue).sum();

            collector.collect(new Tuple2(key.get(), String.valueOf(aDouble)));
        }
    }

}
