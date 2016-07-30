package org.bdp.string_sim.types;

import org.apache.flink.api.java.tuple.Tuple4;

/**
 * It is recommended to use POJOs instead of TupleX for data types with many fields.
 * Also, POJOs can be used to give large Tuple-types a name.
 * This type is used for id label comparison.
 * Field 0: the id of label A
 * Field 1: the label A
 * Field 2: the id of label B
 * Field 3: the label B
 */
public class IdLabelCompareTuple4 extends Tuple4<Integer, String, Integer, String> {

    /**
     * Standard Constructor.
     */
    public IdLabelCompareTuple4() {
        super();
    }

    /**
     * Extended constructor.
     *
     * @param value0 the id of label A as int
     * @param value1 the label A as string
     * @param value2 the id of label B as int
     * @param value3 the label B as string
     */
    public IdLabelCompareTuple4(Integer value0, String value1, Integer value2, String value3) {
        super(value0, value1, value2, value3);
    }
}
