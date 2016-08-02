package org.bdp.string_sim.types;

import org.apache.flink.api.java.tuple.Tuple6;

public class IdTokenizedLabelTuple6 extends Tuple6<Integer,String,String[],Integer,String,String[]> {

    public IdTokenizedLabelTuple6() {
        super();
    }

    public IdTokenizedLabelTuple6(Integer value0, String value1, String[] value2, Integer value3, String value4, String[] value5) {
        super(value0, value1, value2, value3, value4, value5);
    }
}
