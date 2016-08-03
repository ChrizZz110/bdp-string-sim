package org.bdp.string_sim.types;

import org.apache.flink.api.java.tuple.Tuple2;

public class IdTokenizedLabelTuple2 extends Tuple2<Integer,String[]> {
    public IdTokenizedLabelTuple2() {
        super();
    }

    public IdTokenizedLabelTuple2(Integer value0, String[] value1) {
        super(value0, value1);
    }
}
