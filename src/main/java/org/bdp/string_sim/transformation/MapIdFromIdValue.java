package org.bdp.string_sim.transformation;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;

public class MapIdFromIdValue implements MapFunction<Tuple2<Integer,String>,Integer>{
    @Override
    public Integer map(Tuple2<Integer, String> integerStringTuple2) throws Exception {
        return integerStringTuple2.getField(0);
    }
}
