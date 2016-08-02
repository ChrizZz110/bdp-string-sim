package org.bdp.string_sim.transformation;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.util.Collector;
import org.bdp.string_sim.types.ResultTuple5;


public class StringCompareFlatMap implements FlatMapFunction<Tuple4<Integer,String,Integer,String>,ResultTuple5> {

    private boolean collectOnlyMatchingStrings = false;

    public StringCompareFlatMap() {
    }

    /**
     * Constructor creates an StringCompareFlatMap instance.
     * @param collectOnlyMatchingStrings true if only matching strings should collected in the result, false collects all results
     */
    public StringCompareFlatMap(boolean collectOnlyMatchingStrings) {
        this.collectOnlyMatchingStrings = collectOnlyMatchingStrings;
    }

    /**
     * Flat map function which compares field 1 and 3 of the input tuple4 with a simple equals command.
     * The result (0.0 or 1.0) is saved in last field (4) of the resulting tuple5.
     *
     * @param input the input tuple4:
     *              Field 0: the id of label A
     *              Field 1: the label A
     *              Field 2: the id of label B
     *              Field 3: the label B
     * @param collector the collector which collects all result tuple of type ResultTuple5
     * @throws Exception
     */
    @Override
    public void flatMap(Tuple4<Integer,String,Integer,String> input, Collector<ResultTuple5> collector) throws Exception {
        float similarity = 0;
        if(input.getField(1).equals(input.getField(3)))
        {
            similarity = 1;
        }
        if(!collectOnlyMatchingStrings)
        {
            collector.collect(new ResultTuple5(
                    input.getField(0),
                    input.getField(1),
                    input.getField(2),
                    input.getField(3),
                    similarity
            ));
        } else if(similarity == 1) {
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
