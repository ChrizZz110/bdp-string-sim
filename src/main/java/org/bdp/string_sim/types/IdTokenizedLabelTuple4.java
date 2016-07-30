package org.bdp.string_sim.types;

import org.apache.flink.api.java.tuple.Tuple4;

public class IdTokenizedLabelTuple4 extends Tuple4<Integer, String[], Integer, String[]> {
    /**
     * Standard Constructor.
     */
    public IdTokenizedLabelTuple4() {
        super();
    }

    /**
     * Extended constructor.
     *
     * @param value0 the id of label A as int
     * @param value1 the tokenized label A as string[]
     * @param value2 the id of label B as int
     * @param value3 the tokenized label B as string[]
     */
    public IdTokenizedLabelTuple4(Integer value0, String[] value1, Integer value2, String[] value3) {
        super(value0, value1, value2, value3);
    }
}
