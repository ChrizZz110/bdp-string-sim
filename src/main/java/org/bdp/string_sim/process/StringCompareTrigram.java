package org.bdp.string_sim.process;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.util.Collector;
import org.bdp.string_sim.types.ResultTuple5;


public class StringCompareTrigram implements FlatMapFunction<Tuple4<Integer,String,Integer,String>,ResultTuple5> {

    private float thresholdMatch = (float)0.7;

    public StringCompareTrigram() {
    }

    public StringCompareTrigram(float collectOnlyMatchingStrings) {
        this.thresholdMatch = collectOnlyMatchingStrings;
    }

    @Override
    public void flatMap(Tuple4<Integer, String, Integer, String> input, Collector<ResultTuple5> collector) throws Exception {
    	
    }
}