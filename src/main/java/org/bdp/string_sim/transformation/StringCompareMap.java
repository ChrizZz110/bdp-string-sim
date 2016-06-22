package org.bdp.string_sim.transformation;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.util.Collector;
import org.bdp.string_sim.types.ResultTuple5;

/**
 * TODO: Comment me ...
 */
public class StringCompareMap implements FlatMapFunction<Tuple4<Integer,String,Integer,String>,ResultTuple5> {

    private boolean collectOnlyMatchingStrings = false;

    public StringCompareMap() {
    }

    public StringCompareMap(boolean collectOnlyMatchingStrings) {
        this.collectOnlyMatchingStrings = collectOnlyMatchingStrings;
    }

    @Override
    public void flatMap(Tuple4<Integer, String, Integer, String> input, Collector<ResultTuple5> collector) throws Exception {
        float similarity = 0;
        if(input.getField(1).equals(input.getField(3)))
        {
            similarity = 1;
        }
        if(collectOnlyMatchingStrings && similarity == 1)
        {
            collector.collect(new ResultTuple5(
                    input.getField(0),
                    input.getField(1),
                    input.getField(2),
                    input.getField(3),
                    similarity
            ));
        } else if(!collectOnlyMatchingStrings)
        {
            collector.collect(new ResultTuple5(
                    input.getField(0),
                    input.getField(1),
                    input.getField(2),
                    input.getField(3),
                    similarity
            ));
        }

    }
}
