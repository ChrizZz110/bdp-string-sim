package org.bdp.string_sim.types;

import org.apache.flink.api.java.tuple.Tuple5;

/**
 * It is recommended to use POJOs instead of TupleX for data types with many fields.
 * Also, POJOs can be used to give large Tuple-types a name.
 * This type is used for the output tuple after the similarity check.
 * Field 0: the id of label A
 * Field 1: the label A
 * Field 2: the id of label B
 * Field 3: the label B
 * Field 4: the similarity value as float
 */
public class ResultTuple5 extends Tuple5<Integer,String,Integer,String,Float> {
    /**
     * Standard Constructor.
     */
    public ResultTuple5() {
        super();
    }

    /**
     *
     * @param value0 the id of label A as int
     * @param value1 the label A as string
     * @param value2 the id of label B as int
     * @param value3 the label B as string
     * @param value4 the similarity value as float
     */
    public ResultTuple5(Integer value0, String value1, Integer value2, String value3, Float value4) {
        super(value0, value1, value2, value3, value4);
    }
}
