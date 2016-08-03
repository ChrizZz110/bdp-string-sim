package org.bdp.string_sim.transformation;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import org.bdp.string_sim.types.IdTokenizedLabelTuple2;

public class CollectTokenFromTuple2FlatMap implements FlatMapFunction<IdTokenizedLabelTuple2, String> {
    @Override
    public void flatMap(IdTokenizedLabelTuple2 integerTuple2, Collector<String> collector) throws Exception {
        String[] tokens = integerTuple2.getField(1);
        for(String token : tokens){
            collector.collect(token);
        }
    }
}
