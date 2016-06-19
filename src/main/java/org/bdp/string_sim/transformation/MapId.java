package org.bdp.string_sim.transformation;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple4;


public class MapId implements MapFunction<Tuple4<Integer,String,String,String>,Integer> {
    /**
     * This MapFunction class is applicable to the flink transformation map(). It maps a Tuple4 to a Integer while ignoring the second, third and fourth element of the tuple.
     * Example: {1,'label',Leipzig,string} is mapped to {1}
     *
     * @param tuple4 the input tuple from the concept_attribute dataset of type Tuple4<Integer, String, String, String>
     * @return Integer the id of the entity
     * @throws Exception
     */
    @Override
    public Integer map(Tuple4<Integer, String, String, String> tuple4) throws Exception {
        return tuple4.getField(0);
    }
}
