package org.bdp.string_sim.transformation;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.util.Collector;
import org.bdp.string_sim.types.ResultTuple5;

public class SortMergeFlatMap implements FlatMapFunction<Tuple4<Integer,String,Integer,String>,ResultTuple5> {

    private double threshold;

    public SortMergeFlatMap(){

    }

    public SortMergeFlatMap(double threshold) {
        this.threshold = threshold;
    }

    @Override
    public void flatMap(Tuple4<Integer, String, Integer, String> input, Collector<ResultTuple5> collector) throws Exception {

    }
}
