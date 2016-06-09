package org.bdp.string_sim.transformation;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;


public class MapId implements MapFunction<Tuple4<Integer,String,String,String>,Integer> {
    @Override
    public Integer map(Tuple4<Integer, String, String, String> tuple4) throws Exception {
        return tuple4.getField(0);
    }
}
