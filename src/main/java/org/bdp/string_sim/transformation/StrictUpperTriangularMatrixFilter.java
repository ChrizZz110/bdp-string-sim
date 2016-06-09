package org.bdp.string_sim.transformation;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.tuple.Tuple2;


public class StrictUpperTriangularMatrixFilter implements FilterFunction<Tuple2<Integer, Integer>> {

    @Override
    public boolean filter(Tuple2<Integer, Integer> integerIntegerTuple2) throws Exception {
        return (Integer) integerIntegerTuple2.getField(0) > (Integer) integerIntegerTuple2.getField(1);
    }
}
