package com.lyf.studyflink.example;

import com.lyf.studyflink.vo.WC;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.operators.MapPartitionOperator;
import org.apache.flink.api.java.tuple.Tuple2;
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
public class WordCountTableMapPartition {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.createCollectionsEnvironment();

        DataSet<WC> input = env.fromElements(
                new WC("120.09", "1"),
                new WC("130.09", "1"),
                new WC("110.09", "2"),
                new WC("100.09", "3"),
                new WC("140.09", "1")
        );

        DataSet<Map<String, List<WC>>> mapPartition = input.mapPartition(new PartitionCounter());
        mapPartition.print();
        DataSet<List<Tuple2<String, Double>>> map = mapPartition.map(new MyTuple2Mapper());
        map.print();

    }

    static class PartitionCounter implements MapPartitionFunction<WC, Map<String, List<WC>>> {

        public void mapPartition(Iterable<WC> values, Collector<Map<String, List<WC>>> out) {
            Map<String, List<WC>> res = new HashMap<>();
            for (WC s : values) {
                if(res.containsKey(s.getPfId())){
                    List<WC> wcs = res.get(s.getPfId());
                    wcs.add(s);
                }else{
                    List<WC> list = new ArrayList<>();
                    list.add(s);
                    res.put(s.getPfId(),list);
                }
            }
            out.collect(res);
        }
    }

    static class MyTuple2Mapper implements MapFunction<Map<String, List<WC>>, List<Tuple2<String, Double>>> {

        @Override
        public List<Tuple2<String, Double>> map(Map<String, List<WC>> stringListMap) throws Exception {
            List<Tuple2<String, Double>>  res = new ArrayList<>();
            for (Map.Entry<String, List<WC>> m : stringListMap.entrySet()) {
                String key = m.getKey();
                List<WC> value = m.getValue();
                double sum = value.stream().mapToDouble(wc -> Double.valueOf(wc.getWord())).sum();
                res.add(new Tuple2(key, sum));
            }
            return res;
        }


    }
}
