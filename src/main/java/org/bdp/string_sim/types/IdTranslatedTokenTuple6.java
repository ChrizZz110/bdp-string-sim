package org.bdp.string_sim.types;

import org.apache.flink.api.java.tuple.Tuple6;

public class IdTranslatedTokenTuple6 extends Tuple6<Integer,String,Long[],Integer,String,Long[]> {
    /**
     * Standard Constructor.
     */
    public IdTranslatedTokenTuple6() {
        super();
    }

    /**
     * Extended constructor.
     */
    public IdTranslatedTokenTuple6(Integer value0, String value1, Long[] value2, Integer value3, String value4, Long[] value5) {
        super(value0, value1, value2, value3, value4, value5);
    }
}
