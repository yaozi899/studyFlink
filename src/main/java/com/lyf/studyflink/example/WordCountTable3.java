package com.lyf.studyflink.example;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @description: WordCountTable
 * @author: lyf
 * @create: 2020-06-26 16:13
 */
@Slf4j
public class WordCountTable3 {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.createCollectionsEnvironment();

        DataSet<Integer> input = env.fromElements(1, 2, 3);

        // collector type must be declared
        input.flatMap((Integer number, Collector<String> out) -> {
            StringBuilder builder = new StringBuilder();
            for(int i = 0; i < number; i++) {
                builder.append("a");
                out.collect(builder.toString());
            }
        })
        // provide type information explicitly
                .returns(Types.STRING)
        // prints "a", "a", "aa", "a", "aa", "aaa"
                .print();
    }

}
