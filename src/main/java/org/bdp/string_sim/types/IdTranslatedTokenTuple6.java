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
     *
     * @param value0 the id of label A as int
     * @param value1 the label A as string
     * @param value2 the translated token array Long[]
     * @param value3 the id of label B as int
     * @param value4 the label B as string
     * @param value5 the translated token array Long[]
     */
    public IdTranslatedTokenTuple6(Integer value0, String value1, Long[] value2, Integer value3, String value4, Long[] value5) {
        super(value0, value1, value2, value3, value4, value5);
    }
}
