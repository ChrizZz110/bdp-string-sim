package org.bdp.string_sim.transformation;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.tuple.Tuple2;


public class StrictUpperTriangularMatrixFilter implements FilterFunction<Tuple2<Integer, Integer>> {

    /**
     * This Filter class transformation can be applied in a filter function.
     * It filters all tuples from the dataset, where field 0 is greater than field 1.
     * E.g. [{1,1},{1,2},{1,3},{2,1}] creates the result [{2,1}]
     *
     * @param integerIntegerTuple2 the input tuple = one dataset with two ids
     * @return boolean true if the first field is greater than the second one
     * @throws Exception
     */
    @Override
    public boolean filter(Tuple2<Integer, Integer> integerIntegerTuple2) throws Exception {
        return ((Integer) integerIntegerTuple2.getField(0) > (Integer) integerIntegerTuple2.getField(1));
    }
}
